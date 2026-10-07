(ns hive-milvus.fake-milvus
  "Test-only in-memory milvus-clj client: implements the client ports
   (IMilvusCore, IMilvusAdmin, IMilvusExtras, ILivenessProbe) over an atom of
   collection -> {id record}. No transport, no network.

   `install!` puts one in milvus-clj's client slot (the slot `connect!` fills
   with a real transport) and a matching IEmbedder in hive-milvus's embedding
   port; `uninstall!` restores both."
  (:require [clojure.string :as str]
            [hive-milvus.embed.fake :as fake-embed]
            [hive-milvus.embed.port :as port]
            [hive-milvus.store.index :as index]
            [milvus-clj.client :as client]))

(def collection
  "The single collection the fake embedder routes every type to."
  "hive_mcp_memory")

(defn- id-filter
  "The id a scalar filter of the form `id == \"x\"` selects, or nil for any
   other filter. Plain string slicing, no regex."
  [filter-expr]
  (let [prefix "id == \""]
    (when (and (string? filter-expr)
               (str/starts-with? filter-expr prefix)
               (str/ends-with? filter-expr "\""))
      (subs filter-expr (count prefix) (dec (count filter-expr))))))

(defn- missing! [coll]
  (throw (ex-info (str "collection not found[collection=" coll "]") {:status 100})))

(defn- rows-of [db coll]
  (or (get @db coll) (missing! coll)))

(defrecord FakeClient [db]
  client/IMilvusCore
  (-has-collection [_ coll] (contains? @db coll))
  (-load-collection [_ coll] {:collection-name coll :status :loaded})
  (-insert [this coll records opts] (client/-upsert this coll records opts))
  (-upsert [_ coll records _opts]
    (rows-of db coll)
    (swap! db update coll into (map (juxt :id identity)) records)
    {:count (count records) :mutation :upsert})
  (-delete [_ coll ids _opts]
    (rows-of db coll)
    (swap! db update coll #(apply dissoc % ids))
    {:deleted (count ids)})
  (-get [_ coll ids _opts]
    (let [rows (rows-of db coll)]
      (into [] (keep rows) ids)))
  (-query [_ coll _q] (rows-of db coll) [])
  (-query-scalar [_ coll {:keys [filter limit]}]
    (let [rows (rows-of db coll)
          hits (if-let [id (id-filter filter)]
                 (keep rows [id])
                 (vals rows))]
      (vec (cond->> hits limit (take limit)))))
  (-close [_] nil)

  client/IMilvusAdmin
  (-create-collection [_ coll _opts]
    (swap! db update coll #(or % {}))
    {:collection-name coll :status :created})
  (-drop-collection [_ coll]
    (swap! db dissoc coll)
    {:collection-name coll :status :dropped})
  (-describe [_ coll] {:collection-name coll})

  client/IMilvusExtras
  (-list-collections [_] (vec (keys @db)))
  (-release-collection [_ _coll] nil)
  (-flush-collection [_ _coll] nil)
  (-create-index [_ _coll _opts] nil)
  (-drop-index [_ _coll _field] nil)

  client/ILivenessProbe
  (-probe! [_] true))

(defn fake-client
  "A FakeClient over a fresh, empty database."
  []
  (->FakeClient (atom {})))

(def ^:private client-slot
  "milvus-clj.api's client slot: the atom `connect!` resets with a transport."
  @#'milvus-clj.api/default-client)

(defn- embedder
  "Every type routes to `collection`, whose name declares 768 dimensions
   (`naming/dim-of`), so every vector is 768 wide."
  []
  (let [v       (vec (repeat 768 0.1))
        routing {:collection-name collection :dimension 768
                 :max-tokens 2048 :provider-key :fake}]
    (fake-embed/embedder
     {:embed-entry                 (fn [& _] {:ok v})
      :embed-text                  (fn [& _] v)
      :routing-for-type            (constantly routing)
      :routing-for-type+size       (constantly routing)
      :collection-names            (constantly [collection])
      :configured-collection-names (constantly [collection])
      :dimension-for-collection    (constantly 768)
      :provider-available-for?     (constantly true)
      :collection-backed?          (constantly true)})))

(defn install!
  "Install a fresh fake client and the fake embedder. Returns a fn that
   restores what was there before."
  []
  (let [prev-client @client-slot
        prev-embed  (port/current)]
    (reset! client-slot (fake-client))
    (port/set-embedder! (embedder))
    (reset! index/loaded-collections #{})
    (reset! index/indexed-collections #{})
    (fn []
      (reset! client-slot prev-client)
      (port/set-embedder! prev-embed)
      (reset! index/loaded-collections #{})
      (reset! index/indexed-collections #{}))))
