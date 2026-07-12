(ns hive-milvus.store.search.pipeline-test
  "The search pipeline, driven through stub ports. No Milvus, no embedder."
  (:require [clojure.test :refer [deftest testing is]]
            [hive-milvus.store.search.boundary :as b]
            [hive-milvus.store.search.pipeline :as pipeline]
            [hive-milvus.store.search.target :as tgt]))

(defn- row
  "A raw Milvus record, as the boundary hands it to record->entry."
  [id distance]
  {:id           id
   :distance     distance
   :type         "note"
   :tags         "[]"
   :content      (str "body of " id)
   :content_hash ""
   :created      "2026-07-12T00:00:00Z"
   :updated      "2026-07-12T00:00:00Z"
   :duration     "medium"
   :access_count 0})

(defn- ctx
  [{:keys [targets vectors rows]}]
  (pipeline/context
   {:resolver (tgt/fixed-resolver targets)
    :embedder (b/stub-embedder vectors)
    :searcher (b/stub-vector-search rows)}))

(def ^:private old-target (tgt/->target "hive_mcp_memory_1024d" :old))
(def ^:private new-target (tgt/->target "hive-mcp-memory-2560d" :new))

;; ============================================================================
;; The happy path
;; ============================================================================

(deftest a-typed-search-hits-one-space-and-keeps-its-order
  (let [c (ctx {:targets [new-target]
                :vectors {:new [0.1 0.2]}
                :rows    {"hive-mcp-memory-2560d" [(row "b" 2.0) (row "a" 1.0) (row "c" 3.0)]}})
        {:keys [results searched failed]} (pipeline/search c {:text "q" :limit 10})]
    (is (empty? failed))
    (is (= [:new] searched))
    (is (= ["a" "b" "c"] (mapv :id results)) "nearest first")
    (is (= "body of a" (:content (first results))) "entry bodies are rehydrated")))

(deftest an-entry-in-both-spaces-is-returned-once
  (testing "the duplicate created by a non-destructive copy is fused away"
    (let [c (ctx {:targets [old-target new-target]
                  :vectors {:old [0.1] :new [0.2]}
                  :rows    {"hive_mcp_memory_1024d"  [(row "shared" 0.5) (row "only-old" 0.9)]
                            "hive-mcp-memory-2560d" [(row "shared" 0.5) (row "only-new" 0.9)]}})
          {:keys [results searched]} (pipeline/search c {:text "q" :limit 10})]
      (is (= 3 (count results)) "3 distinct ids, not 4 rows")
      (is (= "shared" (:id (first results))) "agreement across spaces promotes it")
      (is (= [:old :new] (:spaces (first results))))
      (is (= [:old :new] searched)))))

;; ============================================================================
;; Failure is data, never silence
;; ============================================================================

(deftest a-failing-target-is-reported-not-swallowed
  (testing "a collection that errors must not quietly shrink the result set"
    (let [c (ctx {:targets [old-target new-target]
                  :vectors {:old [0.1] :new [0.2]}
                  ;; :old has no rows registered -> the stub search errors on it
                  :rows    {"hive-mcp-memory-2560d" [(row "a" 1.0)]}})
          {:keys [results searched failed]} (pipeline/search c {:text "q" :limit 10})]
      (is (= ["a"] (mapv :id results)) "the healthy space still answers")
      (is (= [:new] searched))
      (is (= 1 (count failed)))
      (is (= :old (:space (first failed))))
      (is (= "hive_mcp_memory_1024d" (:collection (first failed))))
      (is (some? (:error (first failed))) "and the error itself is carried"))))

(deftest a-failing-embedder-is-reported-per-space
  (let [c (ctx {:targets [old-target new-target]
                :vectors {:new [0.2]}                    ; no vector for :old
                :rows    {"hive_mcp_memory_1024d"  [(row "x" 1.0)]
                          "hive-mcp-memory-2560d" [(row "a" 1.0)]}})
        {:keys [results failed]} (pipeline/search c {:text "q" :limit 10})]
    (is (= ["a"] (mapv :id results)))
    (is (= [:old] (mapv :space failed)) "the space we could not embed into is named")))

(deftest every-target-failing-is-an-empty-result-with-every-failure-named
  (let [c (ctx {:targets [old-target new-target] :vectors {} :rows {}})
        {:keys [results searched failed]} (pipeline/search c {:text "q" :limit 10})]
    (is (empty? results))
    (is (empty? searched))
    (is (= 2 (count failed)) "silence would have looked exactly like 'no matches'")))

;; ============================================================================
;; The stub default
;; ============================================================================

(deftest no-targets-is-an-empty-search-not-an-error
  (let [c (pipeline/context {:resolver (tgt/empty-resolver)
                             :embedder (b/stub-embedder {})
                             :searcher (b/stub-vector-search {})})
        {:keys [results failed]} (pipeline/search c {:text "q" :limit 10})]
    (is (empty? results))
    (is (empty? failed))))
