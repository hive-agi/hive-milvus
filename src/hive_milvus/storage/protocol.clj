(ns hive-milvus.storage.protocol
  "L0 contract — storage bounded context.

   Two narrow protocols (ISP):

   - `IVectorWrite` — upsert one or more entries to a `CollectionRef`.
   - `IVectorRead`  — search across one or more `CollectionRef`s with
                     fan-out semantics (read may union results from
                     active + sunset collections during drain).

   Both protocols depend only on `ICollectionLocator` (via the ref) —
   they have zero coupling to embedder or router internals. The write
   pipeline (in `storage/write_*.clj`) is responsible for resolving
   spec → ref BEFORE calling `upsert!`; this layer never derives a
   collection from a raw type.

   Reload-safety: `defonce`-guarded."
  (:require [clojure.string]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defonce ^:private -ivectorwrite-defined? (atom false))

(when (compare-and-set! -ivectorwrite-defined? false true)
  (defprotocol IVectorWrite
    "Mutating writes to one collection at a time."

    (upsert! [this ref entries]
      "Upsert a sequence of `entries` (each must include `:vector` of
       length `(:coll/dim ref)`) into the collection at
       `(:coll/name ref)`. Returns a `Result` — `Ok {:upserted N}` or
       `Err {:err/tag :storage/* :err/cause _}`. Schema mismatches
       (vector dim ≠ ref dim) surface as
       `:err/tag :embedder/dim-mismatch` to give callers actionable
       fix-direction.")))

(defonce ^:private -ivectorread-defined? (atom false))

(when (compare-and-set! -ivectorread-defined? false true)
  (defprotocol IVectorRead
    "Read fan-out across collections."

    (search [this refs query opts]
      "Search across `refs` (sequence of `CollectionRef`) with the
       given `query` (a vector matching at least one ref's dim).
       Returns a `Result` of merged hits. Implementations MUST skip
       refs whose dim does not match the query's dim — fan-out across
       multiple dims is a caller error, surfaced via
       `:err/tag :storage/dim-mismatch-query`.")))
