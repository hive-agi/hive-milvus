(ns hive-milvus.store.write
  "Single-entry writes and acknowledged write receipts."
  (:require [hive-milvus.store.deref :as d]
            [hive-milvus.store.schema :as schema]
            [hive-milvus.store.routing :as routing]
            [hive-milvus.store.guard :as guard]
            [hive-milvus.resilience.budget :as budget]
            [hive-milvus.resilience.retry :refer [resilient-within]]
            [milvus-clj.api :as milvus]))

(defn- write-entry!
  "Write once and project the acknowledged record through `result`.
   Refusals and retry failures bypass the projection unchanged."
  [config-atom entry result]
  (or (guard/refusal (schema/entry->record-pure entry nil nil))
      (resilient-within config-atom budget/reconnect-wait-ms
        (let [coll-name (routing/ensure-routed! (:type entry))
              record (schema/entry->record entry coll-name)]
          (d/deref! :add (milvus/add coll-name [record] :upsert? true)
                    budget/write-call-timeout-ms)
          (result record)))))

(defn add-entry!
  "Write an entry and return its id, or the ordinary write failure map."
  [config-atom entry]
  (write-entry! config-atom entry :id))

(defn add-entry-returning!
  "Write and return {:id id :entry entry} after acknowledgement.
   Decode the actual stored record as get-entry decodes a row: defaults,
   JSON coercion, generated id and timestamps survive, but no vector escapes.
   No cache or read-back RPC. Refusals retain the ordinary failure shape."
  [config-atom entry]
  (write-entry! config-atom entry
                (fn [record]
                  {:id (:id record) :entry (schema/record->entry record)})))