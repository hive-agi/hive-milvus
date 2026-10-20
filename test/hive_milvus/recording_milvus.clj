(ns hive-milvus.recording-milvus
  "Test-only recording decorator over a milvus-clj client: every port call
   is appended to `calls` as [op collection n-records] and then delegated.
   `install!` puts a recording FakeClient in the client slot via
   hive-milvus.fake-milvus and answers {:calls atom :restore fn}."
  (:require [hive-milvus.fake-milvus :as fake]
            [milvus-clj.client :as client]))

(defn- rec! [calls op coll & [n]]
  (swap! calls conj [op coll n]))

(defrecord RecordingClient [inner calls fail?]
  client/IMilvusCore
  (-has-collection [_ c] (rec! calls :has-collection c) (client/-has-collection inner c))
  (-load-collection [_ c] (rec! calls :load-collection c) (client/-load-collection inner c))
  (-insert [_ c rs o] (rec! calls :insert c (count rs)) (client/-insert inner c rs o))
  (-upsert [_ c rs o]
    (rec! calls :upsert c (count rs))
    (when (and fail? (fail? c rs))
      (throw (ex-info "recorded upsert refused" {:collection c})))
    (client/-upsert inner c rs o))
  (-delete [_ c ids o] (rec! calls :delete c) (client/-delete inner c ids o))
  (-get [_ c ids o] (rec! calls :get c) (client/-get inner c ids o))
  (-query [_ c q] (rec! calls :query c) (client/-query inner c q))
  (-query-scalar [_ c q] (rec! calls :query-scalar c) (client/-query-scalar inner c q))
  (-close [_] (client/-close inner))

  client/IMilvusAdmin
  (-create-collection [_ c o] (rec! calls :create-collection c) (client/-create-collection inner c o))
  (-drop-collection [_ c] (rec! calls :drop-collection c) (client/-drop-collection inner c))
  (-describe [_ c] (rec! calls :describe c) (client/-describe inner c))

  client/IMilvusExtras
  (-list-collections [_] (rec! calls :list-collections nil) (client/-list-collections inner))
  (-release-collection [_ c] (client/-release-collection inner c))
  (-flush-collection [_ c] (client/-flush-collection inner c))
  (-create-index [_ c o] (rec! calls :create-index c) (client/-create-index inner c o))
  (-drop-index [_ c f] (client/-drop-index inner c f))

  client/ILivenessProbe
  (-probe! [_] (client/-probe! inner)))

(def ^:private client-slot @#'milvus-clj.api/default-client)

(defn install!
  "Install the fake client and embedder, then wrap the client in a
   RecordingClient. Opts: :fail? (fn [coll records] -> truthy) makes an
   upsert throw after it is recorded. Returns {:calls atom :restore fn}."
  ([] (install! {}))
  ([{:keys [fail?]}]
   (let [restore (fake/install!)
         calls   (atom [])]
     (swap! client-slot #(->RecordingClient % calls fail?))
     {:calls calls :restore restore})))

(defn count-op
  "How many recorded calls were OP (optionally on collection COLL)."
  ([calls op] (count (filter #(= op (first %)) @calls)))
  ([calls op coll] (count (filter #(and (= op (first %)) (= coll (second %))) @calls))))
