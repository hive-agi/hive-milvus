;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.store.search.boundary
  "The effects a search performs: embedding the query, and asking a collection
   for its nearest rows. Both are ports; both return Result."
  (:require [hive-dsl.result :as r]
            [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.collection.naming :as naming]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]
            [milvus-clj.index :as milvus-index]))

(defprotocol IQueryEmbedder
  (-embed [this target text]
    "r/ok query-vector in `target`'s space | r/err :search/embed-failed."))

(defprotocol IVectorSearch
  (-search [this target query-vec filter-expr limit]
    "r/ok rows (nearest first, at most `limit`) | r/err :search/milvus-query-failed.
     Rows carry `schema/default-read-fields` — never the embedding."))

(def memory-metric
  "The metric the memory collections are indexed with. Read from the index that
   creates them so the query metric and the score→distance conversion below
   cannot drift from the index."
  (:metric-type milvus-index/default-memory-index))

(defn score->distance
  "Milvus reports a COSINE/IP index's proximity as a SIMILARITY (higher is
   nearer) and milvus-clj carries it under the :distance key. Every consumer of
   this pipeline — fusion's ascending rank, chroma's merge-and-rerank, proximum's
   1/(1+d) — reads :distance as a distance (lower is nearer). Convert once, here,
   at the only place that knows the metric. L2 is already a distance."
  [metric s]
  (let [s (double s)]
    (case metric
      (:cosine :ip) (max 0.0 (- 1.0 s))
      s)))

(defn- rows->distances
  "Restate each row's proximity in the pipeline's unit. Rows without a proximity
   (scalar-only output fields) are left untouched."
  [metric rows]
  (mapv (fn [row]
          (cond-> row
            (some? (:distance row)) (update :distance #(score->distance metric %))))
        rows))

(defrecord CollectionEmbedder []
  IQueryEmbedder
  (-embed [_ target text]
    (r/let-ok [query-vec (r/try-effect* :search/embed-failed
                           (embed-svc/embed-for-collection (:collection target) text))]
      ;; A vector of the wrong width is not a degraded answer, it is a wrong one:
      ;; fail loudly rather than search a space we were not embedded into.
      (let [expected (naming/dim-of (:collection target))
            actual   (count query-vec)]
        (if (and expected (not= expected actual))
          (r/err :embedder/dim-mismatch
                 {:collection (:collection target)
                  :space      (:space target)
                  :expected   expected
                  :actual     actual})
          (r/ok query-vec))))))

(defrecord MilvusVectorSearch []
  IVectorSearch
  (-search [_ target query-vec filter-expr limit]
    (r/try-effect* :search/milvus-query-failed
      (rows->distances
       memory-metric
       @(milvus/query (:collection target)
                      (cond-> {:vector        query-vec
                               :limit         limit
                               :metric-type   memory-metric
                               :output-fields schema/default-read-fields}
                        filter-expr (assoc :filter filter-expr)))))))

(defrecord StubEmbedder [vec-by-space]
  IQueryEmbedder
  (-embed [_ target _]
    (if-let [v (get vec-by-space (:space target))]
      (r/ok v)
      (r/err :search/embed-failed {:space (:space target)}))))

(defrecord StubVectorSearch [rows-by-collection]
  IVectorSearch
  (-search [_ target _ _ limit]
    (if-let [rows (get rows-by-collection (:collection target))]
      (r/ok (vec (take limit rows)))
      (r/err :search/milvus-query-failed {:collection (:collection target)}))))

(defn collection-embedder [] (->CollectionEmbedder))
(defn milvus-vector-search [] (->MilvusVectorSearch))
(defn stub-embedder [vec-by-space] (->StubEmbedder vec-by-space))
(defn stub-vector-search [rows-by-collection] (->StubVectorSearch rows-by-collection))
