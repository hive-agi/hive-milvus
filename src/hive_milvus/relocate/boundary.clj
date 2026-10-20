;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate.boundary
  "BOUNDARY: thin Result-tracked wrappers around Milvus side effects
   used by the relocation pipeline. Each fn does ONE Milvus call,
   wrapped in `r/try-effect*` so failures stay railway-tracked
   instead of escaping as exceptions.

   These replace the bare `@(milvus/...)` + bare `try`/`catch`
   patterns in `hive-milvus.store.entries` for the routing-aware
   path. Pre-existing entries.clj fns are not modified here (out of
   scope per the refactor plan)."
  (:require [hive-milvus.store.deref :as d]
            [hive-dsl.result :as r]
            [hive-cppb.core :as cppb]
            [hive-milvus.store.routing :as store-routing]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]
            [hive-milvus.relocate.promoters.record :as p-record]
            [hive-milvus.collection.naming :as naming]))

(defn- ensure-target!
  "Make sure the target collection exists with the correct dimension
   before we write to it. Idempotent — `routing/ensure-routed!` is
   itself a no-op when the collection is present and loaded."
  [memory-type]
  (r/try-effect* :boundary/ensure-target-failed
    (store-routing/ensure-routed! memory-type)))

(cppb/defpromoter ensure-target-collection
  "Promote step that calls `ensure-routed!` for the entry's :type so
   subsequent `milvus-write!` won't 1804 on a missing collection.

   Bundle in:  {:entry e …}
   Bundle out: same (no new keys; effect-only step, threaded for
   short-circuit on failure)."
  [{:keys [entry] :as bundle}]
  (let [res (ensure-target! (:type entry))]
    (if (r/ok? res)
      (r/ok bundle)
      res)))

(defn- read-stored-embedding
  "Result<the vector stored for `id` in `coll`, or nil>. One Milvus query."
  [coll id]
  (r/try-effect* :boundary/embedding-read-failed
    (some-> (d/deref! :query-scalar (milvus/query-scalar coll {:filter            (str "id == \"" id "\"")
                                        :limit             1
                                        :output-fields     ["id" "embedding"]
                                        :consistency-level :strong}))
            first
            :embedding
            vec)))

(defn keep-vector
  "`bundle` with `:kept-embedding` when its stored vector is still the right
   one (`p-record/vector-still-valid?`) and `read` (coll id -> Result<vector>)
   returns one of the collection's width. Anything else, a failed read
   included, leaves the bundle as it is and the record gets a fresh embed:
   keeping the vector only ever saves work, it never decides correctness."
  [read {:keys [src-coll id] :as bundle}]
  (if-not (p-record/vector-still-valid? bundle)
    bundle
    (let [res      (read src-coll id)
          v        (when (r/ok? res) (:ok res))
          expected (naming/dim-of src-coll)]
      (if (and (seq v) (or (nil? expected) (= expected (count v))))
        (assoc bundle :kept-embedding v)
        bundle))))

(cppb/defpromoter keep-stored-vector
  "Promote step before `build-target-record`: read the stored vector into
   `:kept-embedding` when the update leaves the text to embed unchanged, so
   the record is built without calling the embedder. Always r/ok.

   Bundle in:  {:src-coll s :target-coll s :id id :existing e :entry e …}
   Bundle out: same, plus {:kept-embedding v} when kept."
  [bundle]
  (r/ok (keep-vector read-stored-embedding bundle)))

(cppb/defboundary milvus-write!
  [bundle-result]
  :doc
  "Boundary: upsert `:record` into `:target-coll` via Milvus add.
   Returns r/ok bundle on success or r/err :boundary/milvus-write-failed.

   Anaphoric `value` = the bundle map; we read :target-coll and :record
   off of it. On failure, the entry is NOT in target — caller must
   decide whether to retry or roll forward (relocate is idempotent
   when run on the same id later)."
  :on-success
  (let [{:keys [target-coll record]} value]
    (r/try-effect* :boundary/milvus-write-failed
      (do (d/deref! :add (milvus/add target-coll [record] :upsert? true))
          value)))
  :on-failure error)

(defn milvus-write-records!
  "Boundary: upsert `records` into `coll` in ONE Milvus add. Result<count>
   or r/err :boundary/milvus-write-failed. The batched twin of
   `milvus-write!`, used by `pipeline/commit-page`."
  [coll records]
  (r/try-effect* :boundary/milvus-write-failed
    (do (d/deref! :add (milvus/add coll (vec records) :upsert? true))
        (count records))))

(cppb/defboundary milvus-delete!
  [bundle-result]
  :doc
  "Boundary: delete the entry's id from its src collection AFTER a
   successful target write. Failures here are LOGGED but do NOT
   roll back the write — duplicate row in the source is preferred
   over data loss. Returns r/ok bundle so the pipeline finalizes
   even when source delete fails."
  :on-success
  (let [{:keys [src-coll entry]} value
        id (:id entry)]
    (try
      (d/deref! :delete (milvus/delete src-coll [id]))
      (r/ok value)
      (catch Exception e
        (log/warn "milvus-delete! failed; duplicate row remains in"
                  src-coll "for id" id "—" (.getMessage e))
        (r/ok (assoc value :src-delete-failed? true
                           :src-delete-error  (.getMessage e))))))
  :on-failure error)
