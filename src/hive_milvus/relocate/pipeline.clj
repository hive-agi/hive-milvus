;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate.pipeline
  "PIPELINE: compose the collectors + promoters + boundaries into the
   single-entry relocate operation. All-Result; first :err short-circuits.

   The flow:

     collect-existing-entry           ;; COLLECT
       → compute-target               ;; PROMOTE (pure)
       → classify-relocation-need     ;; PROMOTE (pure)
       → IF :no-op → return r/ok with :moved? false
         IF :move  → ensure-target-collection  ;; PROMOTE/BOUNDARY edge
                   → build-target-record       ;; PROMOTE (calls embedder)
                   → milvus-write!              ;; BOUNDARY
                   → milvus-delete!             ;; BOUNDARY
                   → return r/ok with :moved? true

   Replaces the body of `hive-milvus.store.entries/relocate-entry!`
   with a Result-tracked, layer-disciplined version."
  (:require [hive-dsl.result :as r]
            [hive-milvus.relocate.boundary :as boundary]
            [hive-milvus.relocate.collectors.entry :as col-entry]
            [hive-milvus.relocate.promoters.record :as p-record]
            [hive-milvus.relocate.promoters.routing :as p-routing]))

(defprotocol ISourceDisposition
  (-after-write [this bundle]
    "What becomes of the source row once the target write has landed.")
  (-describe-disposition [this]))

(defrecord DeleteSource []
  ISourceDisposition
  (-after-write [_ bundle] (boundary/milvus-delete! (r/ok bundle)))
  (-describe-disposition [_] :move))

(defrecord KeepSource []
  ISourceDisposition
  (-after-write [_ bundle] (r/ok bundle))
  (-describe-disposition [_] :copy))

(defn delete-source
  "Move: the source row is removed once the target write has landed."
  []
  (->DeleteSource))

(defn keep-source
  "Copy: the source row is left exactly where it is. The old collection stays a
   readable backup, at the cost of holding the entry twice."
  []
  (->KeepSource))

(defn place-one
  "Put the entry at `id` into its canonical collection, then let `disposition`
   decide what happens to the source row.

   Returns:
     (r/ok {:placed? true  :source-kept? bool :from src :to target :id id})
     (r/ok {:placed? false :from src :to target :id id})          on no-op
     (r/err :collector/not-found | :routing/no-target |
            :embedder/embed-failed | :boundary/milvus-write-failed | …)

   Idempotent: the target write is an upsert, and an entry already in its
   canonical collection is a no-op."
  [config-atom id disposition]
  (r/let-ok
    [bundle-collected (col-entry/collect-existing-entry {:config-atom config-atom :id id})
     bundle-targeted  (p-routing/compute-target bundle-collected)
     bundle-classed   (p-routing/classify-relocation-need bundle-targeted)]
    (case (:relocation bundle-classed)
      :no-op
      (r/ok {:placed? false
             :from    (:src-coll bundle-classed)
             :to      (:target-coll bundle-classed)
             :id      id})

      :move
      (r/let-ok
        [bundle-ensured  (boundary/ensure-target-collection bundle-classed)
         bundle-recorded (p-record/build-target-record bundle-ensured)
         bundle-written  (boundary/milvus-write! (r/ok bundle-recorded))
         bundle-final    (-after-write disposition bundle-written)]
        (r/ok (cond-> {:placed?      true
                       :source-kept? (= :copy (-describe-disposition disposition))
                       :from         (:src-coll bundle-final)
                       :to           (:target-coll bundle-final)
                       :id           id}
                (:src-delete-failed? bundle-final)
                (assoc :src-delete-failed? true
                       :src-delete-error (:src-delete-error bundle-final))))))))

(defn relocate-one
  "Relocate the entry at `id` to its canonical collection, REMOVING it from the
   source. See `place-one`; `:moved?` mirrors `:placed?` for legacy callers."
  [config-atom id]
  (let [res (place-one config-atom id (delete-source))]
    (if (r/ok? res)
      (r/ok (let [v (:ok res)] (assoc v :moved? (:placed? v))))
      res)))

(defn copy-one
  "Copy the entry at `id` into its canonical collection, LEAVING the source row
   in place. Non-destructive: the old collection remains a complete backup."
  [config-atom id]
  (let [res (place-one config-atom id (keep-source))]
    (if (r/ok? res)
      (r/ok (let [v (:ok res)] (assoc v :moved? (:placed? v))))
      res)))

(defn relocate-update
  "Update-with-relocation: merge `updates` into the entry, recompute
   its target collection from the merged shape, and either upsert in
   place (target == src) or move via `relocate-one` (target ≠ src).

   Replaces the body of `entries/update-entry!` for the routing-aware
   path. Returns r/ok merged-entry on success or r/err on failure.

   Note: `relocate-one` only relocates the entry as it CURRENTLY is.
   When updates change the :type and the new :type routes to a
   different collection, we do NOT use relocate-one — we explicitly
   write the merged entry to the new collection, then delete from src.
   This subtle difference is why `relocate-update` is its own fn
   rather than a wrapper over `relocate-one`."
  [config-atom id updates]
  (r/let-ok
    [bundle-collected (col-entry/collect-existing-entry
                        {:config-atom config-atom :id id})]
    ;; Pure transformations stay in plain `let` — `r/let-ok`
    ;; short-circuits on any binding whose value isn't a Result, so
    ;; raw maps from `merge`/`assoc` would silently abort the pipeline
    ;; before `milvus-write!` ever fires.
    (let [merged        (merge (:entry bundle-collected) updates)
          bundle-merged (assoc bundle-collected :entry merged)]
      (r/let-ok
        [bundle-targeted (p-routing/compute-target bundle-merged)
         bundle-classed  (p-routing/classify-relocation-need bundle-targeted)
         bundle-ensured  (boundary/ensure-target-collection bundle-classed)
         bundle-recorded (p-record/build-target-record bundle-ensured)
         bundle-written  (boundary/milvus-write! (r/ok bundle-recorded))]
        (if (= :no-op (:relocation bundle-classed))
          (r/ok merged)
          (r/let-ok [_bundle-deleted (boundary/milvus-delete! (r/ok bundle-written))]
            (r/ok merged)))))))
