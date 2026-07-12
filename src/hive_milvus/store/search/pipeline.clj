;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.store.search.pipeline
  "Compose a semantic search from its ports: resolve targets, embed the query
   into each, search each, fuse the rankings."
  (:require [hive-dsl.result :as r]
            [hive-milvus.store.search.boundary :as b]
            [hive-milvus.store.search.fusion :as fusion]
            [hive-milvus.store.search.target :as tgt]
            [hive-milvus.store.schema :as schema]))

(defn context
  "The collaborators a search runs against: an ITargetResolver, an IQueryEmbedder
   and an IVectorSearch."
  [{:keys [resolver embedder searcher]}]
  {:resolver resolver
   :embedder embedder
   :searcher searcher})

(defn- ask-one
  "Embed `text` into one target's space and fetch that space's nearest rows.
   => r/ok {:space :target :rows [entry+]} | r/err"
  [{:keys [embedder searcher]} target text filter-expr limit]
  (r/let-ok [query-vec (b/-embed embedder target text)
             rows      (b/-search searcher target query-vec filter-expr limit)]
    (r/ok {:space  (:space target)
           :target target
           :rows   (mapv schema/record->entry rows)})))

(defn- hits-of
  "The (id, distance) pairs one space returned — all fusion may see."
  [{:keys [space rows]}]
  {:space space
   :hits  (mapv (fn [e] {:id (:id e) :distance (double (or (:distance e) 0.0))}) rows)})

(defn- rehydrate
  "Put the entry bodies back onto the fused ranking."
  [by-id fused]
  (mapv (fn [{:keys [id score spaces distance]}]
          (assoc (get by-id id)
                 :score    score
                 :spaces   spaces
                 :distance distance))
        fused))

(defn search
  "Run `query` against every target the resolver names.

   => {:results  [entry+ …]   fused, deduplicated, best first
       :searched [space …]    spaces that answered
       :failed   [{:space … :collection … :error …} …]}

   `:results` are ordered by fused rank, not by distance. A target that fails is
   reported in `:failed`; it does not shrink `:results` silently."
  [ctx {:keys [text limit] :or {limit 10} :as query}]
  (let [targets     (tgt/-targets (:resolver ctx) query)
        filter-expr (schema/build-filter-expr
                     (cond-> {:include-expired? false}
                       (:type query)         (assoc :type (:type query))
                       (:project-ids query)  (assoc :project-ids (:project-ids query))
                       (:exclude-tags query) (assoc :exclude-tags (:exclude-tags query))))
        outcomes    (mapv (fn [t] [t (ask-one ctx t text filter-expr limit)]) targets)
        answered    (for [[_ res] outcomes :when (r/ok? res)] (:ok res))
        failed      (for [[t res] outcomes :when (not (r/ok? res))]
                      {:space (:space t) :collection (:collection t) :error (:error res)})
        by-id       (into {} (for [a answered, e (:rows a)] [(:id e) e]))
        fused       (fusion/fuse (mapv hits-of answered) limit)]
    {:results  (rehydrate by-id fused)
     :searched (mapv :space answered)
     :failed   (vec failed)}))
