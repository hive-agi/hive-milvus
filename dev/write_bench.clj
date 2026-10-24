(ns write-bench
  "Isolated JVM scratch-only benchmark for full memory add with embedding stub."
  (:require [clojure.edn :as edn]
            [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]
            [hive-milvus.store :as store]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.write :as write]
            [hive-spi.memory.ports :as memory]
            [milvus-clj.api :as api]
            [milvus-clj.client :as client]
            [milvus-clj.schema :as schema]
            [milvus-clj.index :as mi]))

(defn timed [f]
  (let [start (System/nanoTime) result (f)]
    {:ms (/ (- (System/nanoTime) start) 1e6) :value result}))

(defn observed [events op f]
  (let [{:keys [ms value]} (timed f)]
    (swap! events conj {:op op :ms ms})
    value))

(defrecord ObservedClient [delegate events]
  client/IMilvusCore
  (-has-collection [_ n] (observed events :has #(client/-has-collection delegate n)))
  (-load-collection [_ n] (observed events :load #(client/-load-collection delegate n)))
  (-insert [_ n records opts] (observed events :insert #(client/-insert delegate n records opts)))
  (-upsert [_ n records opts] (observed events :upsert #(client/-upsert delegate n records opts)))
  (-delete [_ n ids opts] (observed events :delete #(client/-delete delegate n ids opts)))
  (-get [_ n ids opts] (observed events :get #(client/-get delegate n ids opts)))
  (-query [_ n opts] (observed events :search #(client/-query delegate n opts)))
  (-query-scalar [_ n opts] (observed events :query #(client/-query-scalar delegate n opts)))
  (-close [_] (client/-close delegate))
  client/IMilvusAdmin
  (-create-collection [_ n opts] (observed events :create #(client/-create-collection delegate n opts)))
  (-drop-collection [_ n] (observed events :drop #(client/-drop-collection delegate n)))
  (-describe [_ n] (observed events :describe #(client/-describe delegate n)))
  client/IMilvusExtras
  (-list-collections [_] (client/-list-collections delegate))
  (-release-collection [_ n] (client/-release-collection delegate n))
  (-flush-collection [_ n] (client/-flush-collection delegate n))
  (-create-index [_ n opts] (client/-create-index delegate n opts))
  (-drop-index [_ n field] (client/-drop-index delegate n field))
  client/ILivenessProbe
  (-probe! [_] (observed events :probe #(client/probe! delegate))))

(defrecord ScratchEmbedder [collection dimensions calls]
  port/IEmbedder
  (-embed-entry [_ _ _ _]
    (swap! calls inc)
    (r/ok (vec (repeat dimensions 0.125))))
  (-embed-text [_ _ _] (vec (repeat dimensions 0.125)))
  (-routing-for-type [_ _] {:collection-name collection :dimension dimensions})
  (-routing-for-type+size [_ _ _] {:collection-name collection :dimension dimensions})
  (-no-embed-type? [_ _] false)
  (-collection-names [_] [collection])
  (-dimension-for-collection [_ _] dimensions)
  (-configured-collection-names [_] [collection])
  (-provider-available-for? [_ _] true)
  (-collection-backed? [_ n] (= n collection)))

(defn summarize [rows]
  (into {}
        (for [arm [:baseline :receipt]
              :let [rs (filter #(= arm (:arm %)) rows)]]
          [arm {:n (count rs)
                :mean-ms (/ (reduce + (map :ms rs)) (count rs))
                :rpc-counts (frequencies (mapcat #(map :op (:requests %)) rs))
                :rpc-mean-ms (into {}
                                   (for [op [:probe :has :upsert :get :query]
                                         :let [xs (keep #(when (= op (:op %)) (:ms %))
                                                        (mapcat :requests rs))]
                                         :when (seq xs)]
                                     [op (/ (reduce + xs) (count xs))]))}])))

(defn fetch-with-retry [backend id]
  (loop [attempt 1]
    (let [entry (memory/get-entry backend id)]
      (cond
        entry entry
        (= attempt 6) nil
        :else (do (Thread/sleep (* attempt 40))
                  (recur (inc attempt)))))))

(defn run! [private-config-file]
  (let [config (edn/read-string (slurp private-config-file))
        _ (assert (= :http (:transport config)))
        collection (str "cx_write_" (System/currentTimeMillis))
        dimensions 2560 events (atom []) calls (atom 0)
        slot (var-get (get (ns-interns 'milvus-clj.api) 'default-client))
        config-atom (atom {:collection-name collection})
        backend (store/->MilvusMemoryStore config-atom)
        old-embedder (port/current)]
    (api/connect! config)
    (try
      @(api/create-collection collection {:schema (schema/with-dimension dimensions)
                                          :index mi/default-memory-index})
      (println :scratch-created collection)
      (reset! slot (->ObservedClient @slot events))
      (port/set-embedder! (->ScratchEmbedder collection dimensions calls))
      (swap! index/loaded-collections conj collection)
      (swap! index/indexed-collections conj collection)
      (letfn [(sample! [arm i]
                (reset! events [])
                (let [entry {:id (str "write-bench-" i "-" (name arm))
                             :type :note :project-id "write-bench"
                             :content-hash (str i "-" (name arm))
                             :content (apply str (repeat 1024 "x")) :tags ["scratch"]}
                      {:keys [ms value]}
                      (timed (fn []
                               (assert (nil? (memory/find-duplicate backend :note (:content-hash entry) {:project-id "write-bench"})))
                               (if (= arm :baseline)
                                 (let [id (memory/add-entry! backend entry)]
                                   (assert (= (:id entry) id) (pr-str id))
                                   (fetch-with-retry backend id))
                                 (:entry (write/add-entry-returning! config-atom entry)))))
                      requests @events]
                  (assert (= (:id entry) (:id value)) (pr-str {:value value :requests requests :known (hive-milvus.store.lookup/known-collections config-atom)}))
                  ;; Validation is outside the timed window and request counts.
                  (when (= arm :receipt)
                    (Thread/sleep 6000)
                    (assert (= value (fetch-with-retry backend (:id entry)))))
                  {:arm arm :ms ms :requests requests :id (:id entry)}))]
        (sample! :baseline -1)
        (sample! :receipt -1)
        (let [rows (vec (for [i (range 5)
                              arm (if (even? i) [:baseline :receipt] [:receipt :baseline])]
                          (let [row (sample! arm i)]
                            (println :sample (pr-str row)) row)))]
          (println :summary (pr-str (summarize rows)))
          (println :embed-calls @calls)))
      (finally
        (port/set-embedder! old-embedder)
        (swap! index/loaded-collections disj collection)
        (swap! index/indexed-collections disj collection)
        (try @(api/drop-collection collection)
             (println :scratch-dropped collection)
             (finally (api/disconnect!)))))))
