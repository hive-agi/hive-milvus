;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.store.search.fusion
  "Merge hits from different embedding spaces. Pure."
  (:require [malli.core :as m]))

(def Distance [:double {:min 0.0 :max 1000000.0}])

(def Hit
  [:map
   [:id       :string]
   [:distance Distance]])

(def SpaceHits
  [:map
   [:space :keyword]
   [:hits  [:sequential Hit]]])

(def Results [:sequential SpaceHits])

(def Limit [:int {:min 0 :max 1000}])

(def rrf-k 60)

(defn rank-hits
  "`hits` ordered nearest-first, each stamped with its 1-based `:rank`.
   Stable on ties."
  [hits]
  (->> hits
       (sort-by :distance)
       (map-indexed (fn [i h] (assoc h :rank (inc i))))
       vec))

(defn rrf-score
  "A rank's contribution to a fused score."
  [rank]
  (/ 1.0 (+ rrf-k rank)))

(defn- contributions
  [results]
  (for [{:keys [space hits]} results
        h                    (rank-hits hits)]
    {:id (:id h) :space space :rank (:rank h) :distance (:distance h)}))

(defn fuse
  "One ranking over every space, best first, at most `limit` long.

   Ids are deduplicated across spaces; an id several spaces returned accumulates
   their reciprocal ranks. `:score` is a rank score, not a distance — the two are
   different units and must never be compared. `:distance` is carried for display
   and is meaningful only within `:best-space`."
  [results limit]
  (->> (contributions results)
       (group-by :id)
       (map (fn [[id cs]]
              {:id         id
               :score      (reduce + (map (comp rrf-score :rank) cs))
               :spaces     (vec (distinct (map :space cs)))
               :best-space (:space (apply min-key :rank cs))
               :distance   (:distance (apply min-key :rank cs))}))
       (sort-by :score >)
       (take limit)
       vec))

(defn single-space?
  "True when every hit came from at most one space."
  [results]
  (<= (count (distinct (map :space results))) 1))

(m/=> rank-hits [:=> [:cat [:sequential Hit]] [:sequential [:map [:id :string] [:rank [:int {:min 1}]]]]])
(m/=> rrf-score [:=> [:cat [:int {:min 1 :max 1000}]] :double])
(m/=> fuse [:=> [:cat Results Limit] [:sequential [:map [:id :string] [:score :double]]]])
(m/=> single-space? [:=> [:cat Results] :boolean])
