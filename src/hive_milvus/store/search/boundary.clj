;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.store.search.boundary
  "The effects a search performs: embedding the query, and asking a collection
   for its nearest rows. Both are ports; both return Result."
  (:require [hive-dsl.result :as r]
            [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]))

(defprotocol IQueryEmbedder
  (-embed [this target text]
    "r/ok query-vector in `target`'s space | r/err :search/embed-failed."))

(defprotocol IVectorSearch
  (-search [this target query-vec filter-expr limit]
    "r/ok rows (nearest first, at most `limit`) | r/err :search/milvus-query-failed.
     Rows carry `schema/default-read-fields` — never the embedding."))

(defrecord CollectionEmbedder []
  IQueryEmbedder
  (-embed [_ target text]
    (r/try-effect* :search/embed-failed
      (embed-svc/embed-for-collection (:collection target) text))))

(defrecord MilvusVectorSearch []
  IVectorSearch
  (-search [_ target query-vec filter-expr limit]
    (r/try-effect* :search/milvus-query-failed
      @(milvus/query (:collection target)
                     (cond-> {:vector        query-vec
                              :limit         limit
                              :output-fields schema/default-read-fields}
                       filter-expr (assoc :filter filter-expr))))))

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
