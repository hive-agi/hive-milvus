;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

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

(defn vector-still-valid?
  "True when the vector stored for `:existing` is the one `:entry` would get:
   the row stays in its collection and the text the embedder would read is
   unchanged. A rewrite that only changes what is stored (a store decorator
   sealing `:content` and handing over the same plaintext as `:embed-text`)
   then keeps its vector instead of paying a re-embed."
  [{:keys [src-coll target-coll existing entry]}]
  (boolean
    (and existing
         (some? src-coll)
         (= src-coll target-coll)
         (= (embedder/text-to-embed existing) (embedder/text-to-embed entry)))))

(cppb/defpromoter build-target-record
  "Re-embed the entry under the target collection's routing-aligned
   provider, then build the Milvus insert record. A bundle carrying
   `:kept-embedding` (the stored vector, still valid per
   `vector-still-valid?`) is built on it and the embedder is not called.

   Bundle in:  {:entry e :target-coll s :kept-embedding v? …}
   Bundle out: same plus {:embedding v :record m}

   Returns r/err :embedder/* on embed failure (propagated from
   `hive-milvus.embedder/embed-for-entry`)."
  [{:keys [entry target-coll kept-embedding] :as bundle}]
  (let [emb-res (if (seq kept-embedding)
                  (r/ok kept-embedding)
                  (embedder/embed-for-entry entry target-coll))]
    (if (r/ok? emb-res)
      (let [embedding (:ok emb-res)
            record    (schema/entry->record-pure entry target-coll embedding)]
        (r/ok (assoc bundle
                     :embedding embedding
                     :record    record)))
      ;; Propagate the embedder's err shape verbatim so the pipeline
      ;; surfaces the original category to callers.
      emb-res)))
