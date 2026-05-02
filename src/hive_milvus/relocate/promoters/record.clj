;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.relocate.promoters.record
  "PROMOTE: wrap the embedder BOUNDARY call + the pure schema record
   builder into a Result-tracked promoter. The single caller (the
   pipeline) gets one r/ok / r/err to thread through.

   Why this is a promoter even though it touches the embedder:
   `embed-for-entry` returns Result; from the pipeline's POV the
   embedding is just data flowing through the bundle. The actual
   I/O (HTTP call to Ollama) is encapsulated in the embedder ns;
   here we only orchestrate."
  (:require [hive-dsl.result :as r]
            [hive-cppb.core :as cppb]
            [hive-milvus.embedder :as embedder]
            [hive-milvus.store.schema :as schema]))

(cppb/defpromoter build-target-record
  "Re-embed the entry under the target collection's routing-aligned
   provider, then build the Milvus insert record.

   Bundle in:  {:entry e :target-coll s …}
   Bundle out: same plus {:embedding v :record m}

   Returns r/err :embedder/* on embed failure (propagated from
   `hive-milvus.embedder/embed-for-entry`)."
  [{:keys [entry target-coll] :as bundle}]
  (let [emb-res (embedder/embed-for-entry entry target-coll)]
    (if (r/ok? emb-res)
      (let [embedding (:ok emb-res)
            record    (schema/entry->record-pure entry target-coll embedding)]
        (r/ok (assoc bundle
                     :embedding embedding
                     :record    record)))
      ;; Propagate the embedder's err shape verbatim so the pipeline
      ;; surfaces the original category to callers.
      emb-res)))
