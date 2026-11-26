(ns hive-milvus.embedding-space-test
  "index/ensure-collection! guards a collection's embedding space, not only
   its dimension: a new collection is stamped with the space in its Milvus
   properties, an existing one must hold the same space (another space throws
   :embedding-space-mismatch), and an unstamped legacy collection is admitted
   with a warning. Driven over an in-memory fake of milvus-clj's admin calls
   and of the v2 REST describe/alter_properties pair; no cluster."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-milvus.collection.ensure :as cens]
            [hive-milvus.embed.port :as port]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.routing :as routing]
            [hive-milvus.store.space :as mspace]
            [hive-spi.embeddings.space :as space]
            [milvus-clj.api :as milvus]))

(def ^:private qwen
  {:provider :ollama :model "qwen3-embedding:4b" :digest "sha256:aaa"
   :dimension 2560 :query-template-rev "raw" :doc-serializer-rev "memory-to-document/1"})

(def ^:private coll "hive_mcp_memory_2560d")

(defn- fake-milvus
  "State of a fake Milvus: {name {:properties {string string}}}, plus the
   REST calls it answered."
  []
  (atom {:collections {} :calls [] :creates 0}))

(defn- with-fake* [m f]
  (with-redefs [milvus/has-collection    (fn [n] (delay (contains? (:collections @m) n)))
                milvus/create-collection (fn [n _]
                                           (swap! m #(-> % (update :creates inc)
                                                         (assoc-in [:collections n] {:properties {}})))
                                           (delay {:collection-name n :status :created}))
                milvus/load-collection   (fn [n] (delay {:collection-name n :status :loaded}))
                index/ensure-scalar-indexes! (fn [_] true)
                mspace/request!          (fn [path body]
                                           (swap! m update :calls conj [path body])
                                           (let [n (:collectionName body)]
                                             (case path
                                               "/v2/vectordb/collections/describe"
                                               {:collectionName n
                                                :properties (mapv (fn [[k v]] {:key k :value v})
                                                                  (get-in @m [:collections n :properties]))}
                                               "/v2/vectordb/collections/alter_properties"
                                               (do (swap! m update-in [:collections n :properties]
                                                          merge (:properties body))
                                                   {}))))]
    (f)))

(defn- reset-memos! []
  (reset! index/loaded-collections #{})
  (reset! index/space-checked #{})
  (mspace/reset-legacy-warnings!))

(use-fixtures :each (fn [t] (reset-memos!) (t) (reset-memos!)))

(defn- stamp-of [m] (mspace/properties->stamp (get-in @m [:collections coll :properties])))

(defn- reopen!
  "A fresh process opening the collection: memos cleared, state kept."
  [space]
  (reset-memos!)
  (index/ensure-collection! coll 2560 space))

(deftest properties-round-trip-the-space
  (let [s (mspace/space-for-collection qwen)]
    (is (= :cosine (:metric s)))
    (is (every? string? (vals (mspace/stamp->properties s))))
    (is (= s (space/metadata->space (mspace/properties->stamp (mspace/stamp->properties s)))))
    (testing "the REST [{key value}] shape and a plain map read the same"
      (is (= (mspace/properties->stamp (mspace/stamp->properties s))
             (mspace/properties->stamp (mapv (fn [[k v]] {:key k :value v})
                                             (mspace/stamp->properties s))))))
    (is (= {} (mspace/properties->stamp nil)))
    (is (= {} (mspace/properties->stamp [{:key "collection.ttl.seconds" :value "0"}])))))

(deftest new-collection-is-stamped
  (let [m (fake-milvus)]
    (with-fake* m #(index/ensure-collection! coll 2560 qwen))
    (is (= 1 (:creates @m)))
    (is (= (space/space-id (mspace/space-for-collection qwen)) (:embed-space-id (stamp-of m))))
    (is (= "sha256:aaa" (:embed-digest (stamp-of m))))
    (is (= 2560 (:embed-dimension (stamp-of m))))))

(deftest no-space-creates-unstamped
  (let [m (fake-milvus)]
    (with-fake* m #(index/ensure-collection! coll 2560))
    (is (= 1 (:creates @m)))
    (is (= {} (stamp-of m)))
    (is (empty? (:calls @m)) "the two-arity sends no provenance call")))

(deftest reopen-with-the-same-space-is-admitted
  (let [m (fake-milvus)]
    (with-fake* m (fn []
                    (index/ensure-collection! coll 2560 qwen)
                    (reopen! qwen)))
    (is (= 1 (:creates @m)))))

(deftest reopen-with-another-digest-is-refused
  (let [m (fake-milvus)]
    (with-fake* m (fn []
                    (index/ensure-collection! coll 2560 qwen)
                    (let [e (try (reopen! (assoc qwen :digest "sha256:bbb")) nil
                                 (catch clojure.lang.ExceptionInfo e e))]
                      (is (some? e) "a same-dimension, other-digest space is not admitted")
                      (is (= :embedding-space-mismatch (:error (ex-data e))))
                      (is (= coll (:collection (ex-data e))))
                      (is (= [:digest] (:differs-on (ex-data e))))
                      (is (= #{:isolate :re-embed} (:remedy (ex-data e)))))))))

(deftest a-loaded-collection-is-still-checked-for-a-new-space
  (testing "the load memo does not let a second space into a collection this process loaded"
    (let [m (fake-milvus)]
      (with-fake* m (fn []
                      (index/ensure-collection! coll 2560 qwen)
                      (is (thrown? clojure.lang.ExceptionInfo
                                   (index/ensure-collection!
                                    coll 2560 (assoc qwen :doc-serializer-rev "memory-to-document/2")))))))))

(deftest the-check-runs-once-per-collection-and-space
  (let [m (fake-milvus)]
    (with-fake* m (fn []
                    (index/ensure-collection! coll 2560 qwen)
                    (dotimes [_ 5] (index/ensure-collection! coll 2560 qwen))))
    (is (= 1 (count (:calls @m))) "only the create-time stamp; no describe per write")))

(deftest legacy-collection-is-admitted-not-stamped
  (let [m (fake-milvus)]
    (swap! m assoc-in [:collections coll] {:properties {}})
    (with-fake* m #(index/ensure-collection! coll 2560 qwen))
    (is (zero? (:creates @m)))
    (is (= {} (stamp-of m)) "a legacy collection is reported, never silently stamped")))

(deftest unreadable-stamp-admits-unverified
  (let [m (fake-milvus)]
    (swap! m assoc-in [:collections coll] {:properties {}})
    (with-fake* m (fn []
                    (with-redefs [mspace/request! (fn [_ _] (throw (ex-info "Milvus index HTTP failure" {:status 500})))]
                      (is (true? (index/ensure-collection! coll 2560 qwen))
                          "nothing throws; the ensure runs on to the scalar indexes"))))))

(deftest routing-names-the-space-through-the-port
  (let [seen (atom nil)
        e    (reify
               port/IEmbedder
               (-routing-for-type [_ _] {:collection-name "hive-mcp-memory-2560d" :dimension 2560
                                         :max-tokens 8192 :provider-key :ollama-qwen3-4b})
               port/IEmbedderSpace
               (-space-for-routing [_ r] (reset! seen r) qwen))]
    (try
      (port/set-embedder! e)
      (is (= qwen (routing/space-for (routing/coll-for-type "note"))))
      (is (= :ollama-qwen3-4b (:provider-key @seen)))
      (testing "the :fallback routing names no space"
        (is (nil? (routing/space-for {:collection-name "hive_mcp_memory" :dimension 768
                                      :provider-key :fallback}))))
      (finally (port/reset-embedder!))))
  (testing "the Noop embedder names no space"
    (is (nil? (port/space-for-routing {:provider-key :x :dimension 4})))))

(deftest collection-ensure-ref-carries-the-space
  (let [m (fake-milvus)]
    (cens/reset-registry!)
    (with-fake* m #(cens/ensure-with-check {:coll/name coll :coll/dim 2560 :coll/space qwen}))
    (is (= "sha256:aaa" (:embed-digest (stamp-of m))))
    (cens/reset-registry!)))
