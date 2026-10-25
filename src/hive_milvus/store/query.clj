(ns hive-milvus.store.query
  "Single-entry read + read-merge-upsert helpers.

   These are the primitives every protocol method composes with when it
   needs to fetch a row by id or mutate it through a read-modify-write
   cycle. They don't own any state — the collection name + id are passed
   through, and the resilient-retry wrapper is applied by the caller."
  (:require [hive-milvus.store.deref :as d]
            [hive-milvus.store.schema :as schema]
            [milvus-clj.api :as milvus]
            [malli.core :as m]))

(defn get-entry-by-id
  "Fetch a single entry by ID from Milvus. Returns entry map or nil.
   Uses STRONG consistency to ensure read-after-write visibility."
  [collection-name id]
  (let [results (d/deref! :get (milvus/get collection-name [id] :consistency-level :strong))]
    (when (seq results)
      (schema/record->entry (first results)))))

(defn get-entry-with-vector
  "Read an update snapshot in one RPC. The stored vector stays outside the
   domain entry, so it cannot leak into update results or metadata."
  [collection-name id]
  (when-let [row (first (d/deref! :get
                         (milvus/get collection-name [id]
                           :include (conj schema/default-read-fields "embedding")
                           :consistency-level :strong)))]
    {:entry (schema/record->entry row)
     :stored-embedding (:embedding row)}))

(defn update-entry-fields!
  "Update an entry by reading, merging, and upserting."
  [collection-name id updates]
  (when-let [existing (get-entry-by-id collection-name id)]
    (let [merged (merge existing updates)
          record (schema/entry->record
                   (assoc merged :id id :updated (schema/now-iso))
                   collection-name)]
      (d/deref! :add (milvus/add collection-name [record] :upsert? true))
      merged)))

(m/=> get-entry-by-id [:=> [:cat :string :any] [:maybe schema/Entry]])
