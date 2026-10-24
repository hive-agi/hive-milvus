(ns scalar-index-bench
  "Scratch-only paired benchmark. Never accepts an existing collection name."
  (:require [clojure.data.json :as json]
            [hive-milvus.store.rest-index :as rest]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.client :as client]
            [milvus-clj.schema :as ms]
            [milvus-clj.index :as mi]
            [clojure.test :as test]
            [hive-milvus.store.entries :as entries]
            [milvus-clj.api :as api]))

(defn timed [f]
  (let [start (System/nanoTime) value (f)]
    {:ms (/ (- (System/nanoTime) start) 1e6) :value value}))

(defn scratch-query! [config collection opts]
  (let [old (hive-milvus.embed.port/current)]
    (try
      (hive-milvus.embed.port/set-embedder!
        (reify hive-milvus.embed.port/IEmbedder
          (-embed-entry [_ _ _ _] (throw (ex-info "not needed" {})))
          (-embed-text [_ _ _] (throw (ex-info "not needed" {})))
          (-routing-for-type [_ _] {:collection-name collection :dimension 32})
          (-routing-for-type+size [_ _ _] {:collection-name collection :dimension 32})
          (-no-embed-type? [_ _] false)
          (-collection-names [_] [collection])
          (-dimension-for-collection [_ _] 32)
          (-configured-collection-names [_] [collection])
          (-provider-available-for? [_ _] true)
          (-collection-backed? [_ n] (= collection n))))
      ;; Deliberately suppress auto-DDL in BOTH benchmark arms. Never real names.
      (swap! hive-milvus.store.index/indexed-collections conj collection)
      (hive-milvus.store.entries/query-entries (atom {:collection-name collection}) opts)
      (finally (hive-milvus.embed.port/set-embedder! old)))))

(defn run! [config-file]
  (let [config (json/read-str (slurp config-file) :key-fn keyword)
        c (client/make (assoc config :transport :http))
        grpc (client/make (assoc config :transport :grpc))
        prefix (str "cx_scalar_" (System/currentTimeMillis))
        a (str prefix "_a") b (str prefix "_b")
        req (partial rest/post! config)
        opts {:schema (ms/with-dimension 32) :index mi/default-memory-index}
        rows (mapv (fn [i]
                     (schema/entry->record-pure
                       {:id (str "row_" i) :type (if (zero? (mod i 10)) :decision :note)
                        :project-id (str "project_" (mod i 97))
                        :tags [(str "scope:project:project_" (mod i 97)) "bench"]
                        :content (apply str (repeat 1024 "x"))
                        :created "2026-01-01T00:00:00Z" :updated "2026-01-01T00:00:00Z"}
                       nil (vec (cons 1.0 (repeat 31 0.0)))))
                   (range 50000))
        cases [{:name :scope-type :opts {:type :decision :project-id "project_0"}}
               {:name :tags-exclude :opts {:tags ["bench"] :exclude-tags ["hidden"]
                                         :project-ids ["project_0" "project_1"]}}
               {:name :type-time :opts {:type :note :created-after "2025-01-01"}}
               {:name :tags-only :opts {:tags ["scope:project:project_0"]}}]]
    (try
      (api/connect! config)
      (doseq [n [a b]]
        (client/-create-collection c n opts)
        (doseq [chunk (partition-all 500 rows)] (client/-insert c n chunk {}))
        (client/-flush-collection grpc n)
        (println :inserted n (req "/v2/vectordb/collections/get_stats" {:collectionName n})))
      (println :build
        (timed #(rest/ensure-fields! req b ["type" "project_id"])))
      (loop [remaining 120]
        (let [states (mapcat #(req "/v2/vectordb/indexes/describe"
                                  {:collectionName b :indexName %})
                            ["type_idx" "project_id_idx"])]
          (println :index-states states)
          (when-not (every? #(= "Finished" (:indexState %)) states)
            (when (zero? remaining) (throw (ex-info "build timed out" {})))
            (Thread/sleep 1000) (recur (dec remaining)))))
      ;; No release/reload: exercise the production rollout's online build.
      (doseq [case cases n [a b]]
        (scratch-query! config n (assoc (:opts case) :limit 100)))
      (let [samples
            (vec (for [iteration (range 8)
                       case cases
                       arm (if (even? iteration) [:before :after] [:after :before])]
                   (let [n (if (= arm :before) a b)
                         f (schema/build-filter-expr (:opts case))
                         result (timed #(scratch-query! config n
                                          (assoc (:opts case) :limit 100)))]
                     (println :sample iteration (:name case) arm (:ms result)
                              :rows (count (:value result)))
                     {:iteration iteration :case (:name case) :arm arm
                      :ms (:ms result) :ids (set (map :id (:value result)))})))]
        (doseq [[k values] (group-by (juxt :case :arm) samples)]
          (println :summary k :mean (/ (reduce + (map :ms values)) (count values))
                   :samples (mapv :ms values)))
        (spit (str "/tmp/" prefix "_results.edn") (pr-str samples)))
      (finally
        (doseq [n [a b]]
          (try (client/-drop-collection c n)
               (println :dropped n)
               (catch Exception e (println :DROP-FAILED n (ex-message e)))))
        (do (api/disconnect!) (client/-close grpc)) (client/-close c)))))
