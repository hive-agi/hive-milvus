(ns hive-milvus.store.deref
  "Bounded deref for milvus-clj RPC futures. Replaces bare `@(milvus/...)`;
   on timeout throws a `:milvus/timeout`-tagged ex-info."
  (:require [hive-weave.safe :as ws]))

(def ^:private default-timeout-ms 5000)

(defn timeout-ms
  "RPC-deref budget in ms. Env MILVUS_DEREF_TIMEOUT_MS, else 5000."
  []
  (or (some-> (System/getenv "MILVUS_DEREF_TIMEOUT_MS") not-empty parse-long)
      default-timeout-ms))

(defn deref!
  "Deref `fut` within `ms` (default `timeout-ms`). Returns the value, or
   throws a `:milvus/timeout`-tagged ex-info on timeout. `op` labels the call."
  ([op fut] (deref! op fut (timeout-ms)))
  ([op fut ms]
   (let [r (ws/deref-safe fut ms ::timeout)]
     (if (identical? r ::timeout)
       (throw (ex-info (str "milvus " (name op) " timed out after " ms "ms")
                       {:milvus/timeout true :op op :timeout-ms ms}))
       r))))
