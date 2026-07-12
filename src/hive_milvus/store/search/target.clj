;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.store.search.target
  "Which collections a search runs against, paired with the embedding space each
   is written in."
  (:require [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.routing :as routing]))

(defrecord SearchTarget [collection space])

(defn ->target
  "A collection and the space it is embedded in. `space` keys the fusion, so it
   must come from the provider, not from the collection name."
  [collection space]
  (->SearchTarget collection space))

(defprotocol ITargetResolver
  (-targets [this query]
    "SearchTargets for `query`. Empty means nothing to search."))

(defrecord RoutedResolver []
  ITargetResolver
  (-targets [_ {:keys [type]}]
    (when type
      (let [{:keys [collection-name provider-key]} (routing/coll-for-type type)]
        [(->target collection-name (or provider-key :unknown))]))))

(defrecord KnownCollectionsResolver [config-atom]
  ITargetResolver
  (-targets [_ _]
    (mapv (fn [coll] (->target coll (keyword coll)))
          (lookup/known-collections config-atom))))

(defrecord FixedResolver [targets]
  ITargetResolver
  (-targets [_ _] targets))

(defrecord EmptyResolver []
  ITargetResolver
  (-targets [_ _] []))

(defrecord FirstMatchResolver [resolvers]
  ITargetResolver
  (-targets [_ query]
    (or (first (keep (fn [r] (seq (-targets r query))) resolvers))
        [])))

(defn routed-resolver [] (->RoutedResolver))
(defn known-collections-resolver [config-atom] (->KnownCollectionsResolver config-atom))
(defn fixed-resolver [targets] (->FixedResolver (vec targets)))
(defn empty-resolver [] (->EmptyResolver))

(defn default-resolver
  "A typed query resolves to its own collection; an untyped one fans out over
   every known collection."
  [config-atom]
  (->FirstMatchResolver [(routed-resolver)
                         (known-collections-resolver config-atom)]))
