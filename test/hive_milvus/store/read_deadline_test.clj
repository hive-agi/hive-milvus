(ns hive-milvus.store.read-deadline-test
  "A read's deadline scales with the ask, and a read that overruns it is an
   error Result - never an empty match. No live Milvus: milvus-clj's calls are
   rebound to stubs."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-milvus.resilience.budget :as budget]
            [hive-milvus.store.deref]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.search.boundary :as b]
            [hive-milvus.store.search.pipeline :as pipeline]
            [hive-milvus.store.search.target :as tgt]
            [milvus-clj.api :as milvus]))

(deftest deadline-grows-with-the-ask-test
  (testing "a small ask stays near the base deadline"
    (is (= 5200 (budget/read-deadline-ms 5000 10 budget/search-row-cost-ms))))
  (testing "a large ask is given time for its rows"
    (is (< 5000 (budget/read-deadline-ms 5000 1000 budget/search-row-cost-ms))))
  (testing "never past the cap, never below the base"
    (is (= budget/read-deadline-cap-ms
           (budget/read-deadline-ms 5000 1000000 budget/search-row-cost-ms)))
    (is (= 5000 (budget/read-deadline-ms 5000 0 budget/search-row-cost-ms)))
    (is (= 5000 (budget/read-deadline-ms 5000 nil 1)))))

(deftest scan-deadline-is-dearer-with-content-test
  (is (< (entries/scan-deadline-ms 10000 ["id"])
         (entries/scan-deadline-ms 10000 ["id" "content"]))))

(def ^:private timed-out
  (ex-info "milvus query-scalar timed out after 5000ms"
           {:milvus/timeout true :op :query-scalar :timeout-ms 5000}))

(deftest a-timed-out-collection-outcome-carries-its-error-test
  (let [o (entries/collection-outcome (fn [_ _] (throw timed-out)) "c" "id != \"\"" 10)]
    (is (string? (:error o)))
    (is (= [] (:rows o)))))

(deftest a-timed-out-scan-is-an-error-result-not-an-empty-match-test
  (let [outcome (entries/collection-outcome (fn [_ _] (throw timed-out)) "c" "x" 10)
        res     (entries/read-outcome (:rows outcome) (:failed (entries/fan-out [outcome])))]
    (is (r/err? res))
    (is (= :milvus/read-incomplete (:error res)))
    (is (= "c" (-> res :failed first :collection)))))

(defn- row [id d]
  {:id id :distance d :type "note" :tags "[]" :content id :content_hash "pfx"
   :created "2026-07-12T00:00:00Z" :updated "2026-07-12T00:00:00Z"
   :duration "medium" :access_count 0})

(deftest search-output-fields-reach-the-searcher-test
  (let [seen (atom nil)
        searcher (reify b/IVectorSearch
                   (-search [this t v f l] (b/-search this t v f l nil))
                   (-search [_ _ _ _ _ opts] (reset! seen opts) (r/ok [(row "a" 0.1)])))
        ctx (pipeline/context {:resolver (tgt/fixed-resolver [(tgt/->target "c" :s)])
                               :embedder (b/stub-embedder {:s [0.1]})
                               :searcher searcher})
        res (entries/search-with ctx "q" {:limit 5 :output-fields ["content_hash"]})]
    (is (= ["a"] (mapv :id res)))
    (is (= {:output-fields ["id" "content_hash"]} @seen))))

(deftest milvus-search-asks-for-a-scaled-deadline-and-its-columns-test
  (let [asked  (atom nil)
        derefs (atom nil)]
    (with-redefs [milvus/query (fn [_ q] (reset! asked q) (delay []))
                  hive-milvus.store.deref/deref!
                  (fn [_op fut ms] (reset! derefs ms) @fut)]
      (b/-search (b/milvus-vector-search) (tgt/->target "c" :s) [0.1] nil 1000
                 {:output-fields ["id" "content_hash"]}))
    (is (= ["id" "content_hash"] (:output-fields @asked)))
    (is (< 5000 @derefs))))
