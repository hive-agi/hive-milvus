(ns hive-milvus.collection.locator
  "L2 — default `ICollectionLocator` impl.

   Thin wrapper composing the pure naming layer (`naming/ref-for-dim`)
   with the sunset-aware config (`config/sunset?`). Like the router,
   reads config lazily on each call so hot-flips propagate.

   The locator is the single source of truth for the
   spec→`CollectionRef` mapping. The write pipeline must NEVER
   construct a CollectionRef from a raw type or string — only from a
   resolved `ProviderSpec` via this protocol. That invariant is what
   eliminates the bypass paths that fed the 1804 bug."
  (:require [hive-milvus.collection.config :as cconfig]
            [hive-milvus.collection.naming :as naming]
            [hive-milvus.collection.protocol :as proto]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(def ^:const ^:private known-dims
  "Dims for which the locator surfaces collections in
   `known-collections`. Static for now — Ship 3 will derive this from
   the resolved `:embedder :providers` map so the list shrinks when
   nomic 768 is retired."
  [768 1024 4096])

(defn- annotate-sunset
  "Stamp `:coll/sunset?` on a ref by querying the config."
  [sunset-set ref]
  (assoc ref :coll/sunset? (boolean (contains? sunset-set (:coll/name ref)))))

(defrecord DefaultLocator [config-fn]
  proto/ICollectionLocator
  (collection-for [_ spec]
    (let [sunset (:sunset (config-fn))]
      (annotate-sunset sunset (naming/ref-for-dim (:provider/dim spec)))))

  (active-collections [_]
    (let [sunset (:sunset (config-fn))]
      (->> known-dims
           (map naming/ref-for-dim)
           (map #(annotate-sunset sunset %))
           (remove :coll/sunset?))))

  (known-collections [_]
    (let [sunset (:sunset (config-fn))]
      (->> known-dims
           (map naming/ref-for-dim)
           (map #(annotate-sunset sunset %))))))

(defn make-locator
  "Construct a `DefaultLocator`. Takes an optional 0-arg `config-fn`
   for testability; in production omit it to pick up `cconfig/resolve!`."
  ([] (make-locator cconfig/resolve!))
  ([config-fn]
   (->DefaultLocator config-fn)))
