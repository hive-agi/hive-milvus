;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

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

   Returns r/err :collector/not-found when the id resolves to no
   collection; r/err :collector/entry-vanished when located but the
   subsequent read returns nil (race window).

   Side-effect: one Milvus get call per known collection until the id
   hits. The underlying `find-entry-collection` already wraps Milvus
   calls in try/catch internally so no raw exceptions escape into the
   pipeline."
  [{:keys [config-atom id]}]
  (if-let [src (lookup/find-entry-collection config-atom id)]
    (if-let [existing (query/get-entry-by-id src id)]
      (r/ok {:id          id
             :src-coll    src
             :entry       existing
             :config-atom config-atom})
      (r/err :collector/entry-vanished
             {:id id :src-coll src
              :reason "find-entry-collection located the id but get-entry-by-id returned nil"}))
    (r/err :collector/not-found {:id id})))
