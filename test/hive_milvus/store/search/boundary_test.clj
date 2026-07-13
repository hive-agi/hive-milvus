;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.store.search.boundary-test
  "The units crossing the Milvus boundary. Milvus reports a COSINE index's
   proximity as a SIMILARITY (higher = better) under the :distance key; the
   search pipeline's contract is a DISTANCE (lower = better). The boundary is
   the one place that conversion may happen."
  (:require [clojure.test :refer [deftest testing is]]
            [hive-milvus.embed.fake :as fake]
            [hive-milvus.store.search.boundary :as b]
            [hive-milvus.store.search.pipeline :as pipeline]
            [hive-milvus.store.search.target :as tgt]
            [milvus-clj.api :as milvus]))

(def ^:private coll "hive_mcp_memory_2560d")

(defn- row
  "A raw Milvus row, `similarity` being what a COSINE index reports."
  [id similarity]
  {:id           id
   :distance     similarity
   :type         "note"
   :tags         "[]"
   :content      (str "body of " id)
   :content_hash ""
   :created      "2026-07-12T00:00:00Z"
   :updated      "2026-07-12T00:00:00Z"
   :duration     "medium"
   :access_count 0})

(defn- stub-milvus
  "milvus/query stands in for the server."
  [& rows]
  (fn [_coll _q] (future (vec rows))))

(defn- r2 [x] (when x (/ (Math/round (* 100.0 (double x))) 100.0)))

;; ============================================================================
;; The conversion itself
;; ============================================================================

(deftest score->distance-inverts-similarity-metrics-only
  (testing "cosine/ip similarities become distances"
    (is (= 0.10 (r2 (b/score->distance :cosine 0.90))))
    (is (= 0.70 (r2 (b/score->distance :cosine 0.30))))
    (is (= 0.10 (r2 (b/score->distance :ip 0.90)))))
  (testing "l2 is already a distance and passes through"
    (is (= 0.90 (r2 (b/score->distance :l2 0.90)))))
  (testing "never negative — an IP score may exceed 1.0"
    (is (= 0.0 (r2 (b/score->distance :cosine 1.4))))))

(deftest the-memory-metric-is-read-from-the-index-that-created-the-collection
  (is (= :cosine b/memory-metric)
      "the conversion must key off the real index metric, never a literal"))

;; ============================================================================
;; The boundary
;; ============================================================================

(deftest cosine-similarity-is-inverted-into-a-distance-at-the-boundary
  (with-redefs [milvus/query (stub-milvus (row "far" 0.30) (row "near" 0.90))]
    (let [t    (tgt/->target coll :new)
          rows (:ok (b/-search (b/milvus-vector-search) t [0.1 0.2] nil 10))]
      (is (= [0.70 0.10] (mapv (comp r2 :distance) rows))
          "a 0.90 cosine similarity is a 0.10 distance"))))

(deftest a-row-without-a-distance-is-left-alone
  (with-redefs [milvus/query (stub-milvus (dissoc (row "scalar" 0.0) :distance))]
    (let [t    (tgt/->target coll :new)
          rows (:ok (b/-search (b/milvus-vector-search) t [0.1 0.2] nil 10))]
      (is (= [nil] (mapv :distance rows))))))

;; ============================================================================
;; End to end through the real boundary — the reported bug
;; ============================================================================

(deftest the-best-cosine-hit-ranks-first-end-to-end
  (testing "the pipeline must not hand back the worst of the top-k first"
    (with-redefs [milvus/query (stub-milvus (row "far" 0.30) (row "near" 0.90))]
      (let [ctx (pipeline/context
                 {:resolver (tgt/fixed-resolver [(tgt/->target coll :new)])
                  :embedder (b/stub-embedder {:new [0.1 0.2]})
                  :searcher (b/milvus-vector-search)})
            {:keys [results]} (pipeline/search ctx {:text "q" :limit 10})]
        (is (= ["near" "far"] (mapv :id results)))
        (is (= 0.10 (r2 (:distance (first results))))
            "and the reported :distance is a true distance, 0 = identical")))))

;; ============================================================================
;; The dimension guard — a wrong-width vector is an error, never neighbours
;; ============================================================================

(deftest a-query-vector-of-the-wrong-width-is-a-hard-error
  (fake/with-embedder {:embed-text (fn [_coll _text] (vec (repeat 768 0.1)))}
    (let [res (b/-embed (b/collection-embedder) (tgt/->target coll :new) "q")]
      (is (not (contains? res :ok)))
      (is (= :embedder/dim-mismatch (:error res)))
      (is (= 2560 (:expected res)))
      (is (= 768 (:actual res))))))

(deftest a-query-vector-of-the-right-width-passes
  (fake/with-embedder {:embed-text (fn [_coll _text] (vec (repeat 2560 0.1)))}
    (let [res (b/-embed (b/collection-embedder) (tgt/->target coll :new) "q")]
      (is (= 2560 (count (:ok res)))))))
