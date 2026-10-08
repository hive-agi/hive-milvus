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

(defn prepare-one
  "Everything `place-one` does BEFORE the target write: collect, route,
   classify and, for a move, ensure the target and build the record.
   `overrides` as in `place-one`.

   Returns:
     (r/ok {:prepare/status :ready :bundle b})   b carries :target-coll :record
     (r/ok {:prepare/status :no-op :result m})   m is place-one's no-op answer
     (r/err ...)                                 any collect/route/embed failure"
  ([config-atom id] (prepare-one config-atom id nil))
  ([config-atom id overrides]
   (r/let-ok
     [bundle-collected (col-entry/collect-existing-entry {:config-atom config-atom :id id})
      bundle-targeted  (p-routing/compute-target
                        (cond-> bundle-collected
                          (seq overrides) (update :entry merge overrides)))
      bundle-classed   (p-routing/classify-relocation-need bundle-targeted)]
     (case (:relocation bundle-classed)
       :no-op
       (r/ok {:prepare/status :no-op
              :result         {:placed? false
                               :from    (:src-coll bundle-classed)
                               :to      (:target-coll bundle-classed)
                               :id      id}})

       :move
       (r/let-ok
         [bundle-ensured  (boundary/ensure-target-collection bundle-classed)
          bundle-recorded (p-record/build-target-record bundle-ensured)]
         (r/ok {:prepare/status :ready :bundle bundle-recorded}))))))

(defn- placed-result
  "Pure: the answer for a bundle whose write landed and whose source row
   `disposition` has dealt with."
  [disposition id bundle-final]
  (cond-> {:placed?      true
           :source-kept? (= :copy (-describe-disposition disposition))
           :from         (:src-coll bundle-final)
           :to           (:target-coll bundle-final)
           :id           id}
    (:src-delete-failed? bundle-final)
    (assoc :src-delete-failed? true
           :src-delete-error (:src-delete-error bundle-final))))

(defn- finish-one
  "Hand a written bundle to `disposition`, then build the placed answer.
   Runs only after the target write landed: delete-after-write."
  [disposition id bundle-written]
  (r/let-ok [bundle-final (-after-write disposition bundle-written)]
    (r/ok (placed-result disposition id bundle-final))))

(defn place-one
  "Put the entry at `id` into its canonical collection, then let `disposition`
   decide what happens to the source row. `overrides` (optional) is merged
   into the collected entry before it is re-embedded: `{:embed-text s}` lets
   a caller whose stored `:content` is not embeddable (a store decorator that
   encrypts at rest) supply the text; `:embed-text` never reaches the record.

   Returns:
     (r/ok {:placed? true  :source-kept? bool :from src :to target :id id})
     (r/ok {:placed? false :from src :to target :id id})          on no-op
     (r/err :collector/not-found | :routing/no-target |
            :embedder/embed-failed | :boundary/milvus-write-failed | ...)

   Idempotent: the target write is an upsert, and an entry already in its
   canonical collection is a no-op.

   Composed of `prepare-one` (collect .. build-target-record), the single
   `milvus-write!`, and `finish-one` (disposition). `commit-page` reuses the
   same two halves with one batched write per target collection."
  ([config-atom id disposition] (place-one config-atom id disposition nil))
  ([config-atom id disposition overrides]
   (r/let-ok [prepared (prepare-one config-atom id overrides)]
     (if (= :ready (:prepare/status prepared))
       (r/let-ok [bundle-written (boundary/milvus-write! (r/ok (:bundle prepared)))]
         (finish-one disposition id bundle-written))
       (r/ok (:result prepared))))))

(defprotocol IRecordWriter
  (-write-records [this coll records]
    "Upsert `records` into `coll` in ONE call. Result<count>."))

(defrecord MilvusWriter []
  IRecordWriter
  (-write-records [_ coll records]
    (boundary/milvus-write-records! coll records)))

(defn milvus-writer
  "The production record writer: one Milvus upsert per call."
  []
  (->MilvusWriter))

(defn- ready? [res]
  (and (r/ok? res) (= :ready (:prepare/status (:ok res)))))

(defn- write-group
  "One `-write-records` for every bundle bound to `coll`. When that batch
   fails, each record is retried on its own so every id keeps its own
   outcome: a failed batch never loses which id failed. Returns one
   Result<bundle> per bundle, in order."
  [writer coll bundles]
  (let [batch (-write-records writer coll (mapv :record bundles))]
    (if (r/ok? batch)
      (mapv r/ok bundles)
      (mapv (fn [b]
              (let [one (-write-records writer coll [(:record b)])]
                (if (r/ok? one) (r/ok b) one)))
            bundles))))

(defn commit-page
  "Write a page of prepared entries with ONE upsert per target collection.

   `prepared` is a seq of [id Result] pairs, each Result as `prepare-one`
   answers it (an err or a no-op passes straight through). Ready bundles are
   grouped by :target-coll and written through `writer` (IRecordWriter);
   `disposition` then runs for each bundle whose write landed, so a move
   deletes a source row only after its target write.

   Returns [[id Result<place-one answer>] ...] in the order given."
  [writer disposition prepared]
  (let [ready   (filter (comp ready? second) prepared)
        groups  (group-by (comp :target-coll :bundle :ok second) ready)
        written (into {}
                      (mapcat (fn [[coll pairs]]
                                (map (fn [[id _] w] [id w])
                                     pairs
                                     (write-group writer coll
                                                  (mapv (comp :bundle :ok second) pairs)))))
                      groups)]
    (mapv (fn [[id res]]
            [id (cond
                  (ready? res) (let [w (get written id)]
                                 (if (r/ok? w) (finish-one disposition id (:ok w)) w))
                  (r/ok? res)  (r/ok (:result (:ok res)))
                  :else        res)])
          prepared)))

(defn relocate-one
  "Relocate the entry at `id` to its canonical collection, REMOVING it from the
   source. See `place-one` (and its `overrides`); `:moved?` mirrors `:placed?`
   for legacy callers."
  ([config-atom id] (relocate-one config-atom id nil))
  ([config-atom id overrides]
   (let [res (place-one config-atom id (delete-source) overrides)]
     (if (r/ok? res)
       (r/ok (let [v (:ok res)] (assoc v :moved? (:placed? v))))
       res))))

(defn copy-one
  "Copy the entry at `id` into its canonical collection, LEAVING the source row
   in place. Non-destructive: the old collection remains a complete backup."
  [config-atom id]
  (let [res (place-one config-atom id (keep-source))]
    (if (r/ok? res)
      (r/ok (let [v (:ok res)] (assoc v :moved? (:placed? v))))
      res)))

(defn merge-updates
  "Pure: the entry `existing` with `updates` merged over it, keeping `id` as
   its identity. An :id inside `updates` is ignored, so an update can neither
   rename the entry nor redirect the write onto another row. nil `updates`
   leaves `existing` unchanged apart from the pinned :id."
  [existing updates id]
  (assoc (merge existing updates) :id id))

(defn relocate-update
  "Update-with-relocation: merge `updates` into the entry, recompute
   its target collection from the merged shape, and either upsert in
   place (target == src) or move via `relocate-one` (target ≠ src).

   Replaces the body of `entries/update-entry!` for the routing-aware
   path. Returns r/ok merged-entry on success or r/err on failure.

   `id` is the entry's identity, not an updatable field: an :id inside
   `updates` is overridden, so it can neither redirect the write nor make
   the source delete hit another row.

   An update that stays in place and leaves the text to embed unchanged
   keeps the stored vector (`boundary/keep-stored-vector`) rather than
   calling the embedder again.

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
    (let [existing      (:entry bundle-collected)
          merged        (merge-updates existing updates id)
          bundle-merged (assoc bundle-collected :entry merged :existing existing)]
      (r/let-ok
        [bundle-targeted (p-routing/compute-target bundle-merged)
         bundle-classed  (p-routing/classify-relocation-need bundle-targeted)
         bundle-ensured  (boundary/ensure-target-collection bundle-classed)
         bundle-kept     (boundary/keep-stored-vector bundle-ensured)
         bundle-recorded (p-record/build-target-record bundle-kept)
         bundle-written  (boundary/milvus-write! (r/ok bundle-recorded))]
        (if (= :no-op (:relocation bundle-classed))
          (r/ok merged)
          (r/let-ok [_bundle-deleted (boundary/milvus-delete! (r/ok bundle-written))]
            (r/ok merged)))))))
