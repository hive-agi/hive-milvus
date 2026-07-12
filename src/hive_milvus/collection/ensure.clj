(ns hive-milvus.collection.ensure
  "L3 — `ICollectionEnsure` impl. Idempotent collection creation with
   dim-check.

   Wraps the imperative `hive-milvus.store.index/ensure-collection!`
   in the Result railway and adds an in-process dim-registry that
   rejects re-ensures requesting a different dimension for the same
   collection name, surfacing a dim mismatch at `ensure!` time rather
   than at the first failing upsert."
  (:require [hive-dsl.result :as r]
            [hive-milvus.collection.protocol :as proto]
            [hive-milvus.store.index :as index]
            [taoensso.timbre :as log]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(defonce ^:private dim-registry (atom {}))

(defn- registered-dim [coll-name]
  (get @dim-registry coll-name))

(defn- register-dim! [coll-name dim]
  (swap! dim-registry assoc coll-name dim))

(defn- dim-mismatch
  "Construct the canonical `:collection/dim-mismatch` err that the
   resilience classifier converts to `:err/schema-mismatch`."
  [ref existing-dim]
  (r/err :collection/dim-mismatch
         {:ref           ref
          :existing-dim  existing-dim
          :requested-dim (:coll/dim ref)
          :hint          "Two providers route to the same collection name with different dims; check :embedder :providers"}))

(defn- create-now!
  "Imperative create-or-load delegated to `store.index`. Returns
   `(ok ref)` on success; converts a throw into `:collection/ensure-failed`."
  [{:keys [:coll/name :coll/dim] :as ref}]
  (try
    (index/ensure-collection! name dim)
    (register-dim! name dim)
    (r/ok ref)
    (catch Throwable t
      (log/warn "ensure-collection failed for" name "dim" dim ":" (.getMessage t))
      (r/err :collection/ensure-failed
             {:ref       ref
              :throwable t}))))

(defn ensure-with-check
  "Pure-ish core (the registry read + write are atom ops; the actual
   Milvus call is via `create-now!`). Exposed for unit tests so the
   dim-check policy can be exercised without spinning up Milvus."
  [ref]
  (let [{:coll/keys [name dim]} ref
        existing               (registered-dim name)]
    (cond
      (nil? existing)        (create-now! ref)
      (= existing dim)       (r/ok ref)
      :else                  (dim-mismatch ref existing))))

(defrecord DefaultEnsure []
  proto/ICollectionEnsure
  (ensure! [_ ref] (ensure-with-check ref)))

(defn make-ensure
  "Construct a `DefaultEnsure`. No state per record — the dim-registry
   is process-global since collection identity is server-global."
  []
  (->DefaultEnsure))

(defn reset-registry!
  "Test helper — clear the in-process dim registry."
  []
  (reset! dim-registry {}))
