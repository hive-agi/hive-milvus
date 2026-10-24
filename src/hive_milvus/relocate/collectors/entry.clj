;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate.collectors.entry
  "COLLECT: locate an entry's current physical collection and read its
   record. Wraps the existing `lookup/find-entry-collection` +
   `query/get-entry-by-id` chain in Result envelopes so the pipeline
   sees them as data.

   Imports the leaf `hive-milvus.store.lookup` ns rather than
   `hive-milvus.store.entries` to keep the relocate pipeline cycle-free
   (entries delegates routing-aware ops to the pipeline; the pipeline
   must NOT loop back into entries)."
  (:require [hive-dsl.result :as r]
            [hive-cppb.core :as cppb]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.query :as query]))

(defn collect-update-with
  "Locate an update snapshot through FETCH (coll id -> snapshot or nil).
   Keep the vector on the pipeline bundle, never on its domain :entry.
   Uses the same absence/outage policy as ordinary collection."
  [fetch colls {:keys [id] :as bundle}]
  (let [located (lookup/locate-collection fetch colls id)]
    (if-not (r/ok? located)
      located
      (if-let [{src :collection snapshot :value} (:ok located)]
        (r/ok (assoc bundle :src-coll src
                           :entry (:entry snapshot)
                           :stored-embedding (:stored-embedding snapshot)))
        (r/err :collector/not-found {:id id})))))

(cppb/defcollector collect-existing-entry
  "Locate and read an entry with outcome-honest absence/outage handling.
   With :include-vector? collect the stored vector in the same snapshot
   for an update; ordinary relocation avoids transferring that vector."
  [{:keys [config-atom id include-vector?]}]
  (let [colls (lookup/known-collections config-atom)]
    (if include-vector?
      (collect-update-with query/get-entry-with-vector colls
                           {:id id :config-atom config-atom})
      (let [located (lookup/locate-collection query/get-entry-by-id colls id)]
        (if-not (r/ok? located)
          located
          (if-let [{src :collection existing :value} (:ok located)]
            (r/ok {:id id :src-coll src :entry existing :config-atom config-atom})
            (r/err :collector/not-found {:id id})))))))
