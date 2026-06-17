(ns hive-milvus.collection.protocol
  "L0 contract — collection bounded context.

   Two narrow protocols (ISP):

   - `ICollectionLocator` — spec → `CollectionRef`. Pure lookup. Knows
                            sunset state so writes can avoid retired
                            collections while reads can fan out across
                            both active + sunset (graceful drain).
   - `ICollectionEnsure`  — create-if-missing on the backend with
                            mandatory dim-check. Rejects an `ensure!`
                            whose ref-dim disagrees with an existing
                            collection's dim — the precise check that
                            prevents the silent 1804 schema-mismatch.

   Reload-safety: `defonce`-guarded."
  (:require [clojure.string]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defonce ^:private -icollectionlocator-defined? (atom false))

(when (compare-and-set! -icollectionlocator-defined? false true)
  (defprotocol ICollectionLocator
    "Pure routing of `ProviderSpec` to a Milvus `CollectionRef`."

    (collection-for [this spec]
      "Return the `CollectionRef` for the given `ProviderSpec`.
       Result shape: `{:coll/name str :coll/dim int :coll/sunset? bool}`.")

    (active-collections [this]
      "Sequence of `CollectionRef` for collections that should accept
       writes. Excludes sunset entries.")

    (known-collections [this]
      "Sequence of all `CollectionRef`s including sunset. Used by
       search fan-out to ensure legacy data remains queryable during
       background drain.")))

(defonce ^:private -icollectionensure-defined? (atom false))

(when (compare-and-set! -icollectionensure-defined? false true)
  (defprotocol ICollectionEnsure
    "Effectful sibling — creates a Milvus collection if missing,
     rejects dim mismatches on existing collections."

    (ensure! [this ref]
      "Idempotent. If the collection named `(:coll/name ref)` does not
       exist, create it with vector dim `(:coll/dim ref)`. If it
       exists with a different dim, return
       `(err {:err/tag :collection/dim-mismatch :ref ref :actual-dim N})`.
       Returns `(ok ref)` on success.")))
