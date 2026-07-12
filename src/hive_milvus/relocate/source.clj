;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.relocate.source
  "Where the next ids to relocate come from.

   The drain reads the head of the source collection and excludes what it
   already handled. It relies on relocation removing the row from the source,
   so the head is always fresh work — it never assumes the store returns rows
   in any order."
  (:require [hive-milvus.relocate.plan :as plan]
            [milvus-clj.api :as milvus]))

(defprotocol IIdSource
  (-next-ids [this n excluded]
    "Up to `n` ids awaiting relocation, none of them in `excluded`.
     Empty means the source is drained.")
  (-describe [this]
    "Data description of this source."))

(defrecord MilvusDrainSource [coll]
  IIdSource
  (-next-ids [_ n excluded]
    (->> @(milvus/query-scalar coll
                               {:filter            (plan/exclusion-filter excluded)
                                :limit             n
                                :output-fields     ["id"]
                                :consistency-level :bounded})
         (mapv :id)))
  (-describe [_] {:source :milvus-drain :collection coll}))

(defrecord SeqIdSource [ids-atom]
  IIdSource
  (-next-ids [_ n excluded]
    (let [skip (set excluded)]
      (->> @ids-atom (remove skip) (take n) vec)))
  (-describe [_] {:source :seq :remaining (count @ids-atom)}))

(defrecord NoOpIdSource []
  IIdSource
  (-next-ids [_ _ _] [])
  (-describe [_] {:source :no-op}))

(defn milvus-drain-source
  "Reads the head of a live Milvus collection."
  [coll]
  (->MilvusDrainSource coll))

(defn seq-id-source
  "Reads from an atom holding a vector of ids. Removing an id from the atom
   models the source-side delete a real relocation performs."
  [ids-atom]
  (->SeqIdSource ids-atom))

(defn no-op-source
  "Always drained. The default when no source is supplied."
  []
  (->NoOpIdSource))
