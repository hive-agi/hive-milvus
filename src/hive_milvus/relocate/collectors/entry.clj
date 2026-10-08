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

(cppb/defcollector collect-existing-entry
  "Locate `id` across known Milvus collections, read it back.

   Bundle in:  {:config-atom a :id id}
   Bundle out: {:id id :src-coll s :entry e :config-atom a}

   Returns r/err :collector/not-found when every collection answered and
   none holds the id.

   Side-effect: one Milvus get call per known collection until the id
   hits. The entry the hit read is the entry returned, so there is no
   second read and no race window between locating and reading.

   The locate step is `lookup/locate-collection`, the outcome-honest
   fold. It catches every per-collection exception, so no raw exception
   escapes into the pipeline: when no collection holds `id` and at least
   one collection could not be read, the answer is r/err
   :milvus/read-incomplete (an outage), never :collector/not-found (an
   absence)."
  [{:keys [config-atom id]}]
  (let [located (lookup/locate-collection query/get-entry-by-id
                                          (lookup/known-collections config-atom)
                                          id)]
    (if-not (r/ok? located)
      located
      (if-let [{src :collection existing :value} (:ok located)]
        (r/ok {:id          id
               :src-coll    src
               :entry       existing
               :config-atom config-atom})
        (r/err :collector/not-found {:id id})))))
