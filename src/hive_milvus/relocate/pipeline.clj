;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

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

(defn relocate-one
  "Relocate the entry at `id` from its current collection to the
   canonical target (per current type-routing config).

   Returns:
     (r/ok {:moved? true   :from src :to target :id id})       on move
     (r/ok {:moved? false  :from src :to target :id id})       on no-op
     (r/ok {:moved? true   :from src :to target :id id
            :src-delete-failed? true                            ; partial
            :src-delete-error msg})
     (r/err :collector/not-found    {:id id})
     (r/err :collector/entry-vanished …)
     (r/err :routing/no-target …)
     (r/err :embedder/no-provider …)
     (r/err :embedder/embed-failed …)
     (r/err :boundary/milvus-write-failed …)

   Idempotent: relocate-one on an entry already in target returns
   :no-op without doing any I/O beyond the lookup."
  [config-atom id]
  (r/let-ok
    [bundle-collected (col-entry/collect-existing-entry {:config-atom config-atom :id id})
     bundle-targeted  (p-routing/compute-target bundle-collected)
     bundle-classed   (p-routing/classify-relocation-need bundle-targeted)]
    (case (:relocation bundle-classed)
      :no-op
      (r/ok {:moved? false
             :from   (:src-coll bundle-classed)
             :to     (:target-coll bundle-classed)
             :id     id})

      :move
      (r/let-ok
        [bundle-ensured  (boundary/ensure-target-collection bundle-classed)
         bundle-recorded (p-record/build-target-record bundle-ensured)
         bundle-written  (boundary/milvus-write! (r/ok bundle-recorded))
         bundle-deleted  (boundary/milvus-delete! (r/ok bundle-written))]
        (r/ok (cond-> {:moved? true
                       :from   (:src-coll bundle-deleted)
                       :to     (:target-coll bundle-deleted)
                       :id     id}
                (:src-delete-failed? bundle-deleted)
                (assoc :src-delete-failed? true
                       :src-delete-error (:src-delete-error bundle-deleted))))))))

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
