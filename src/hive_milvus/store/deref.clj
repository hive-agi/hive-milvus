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
   throws a `:milvus/timeout`-tagged ex-info on timeout. `op` labels the call.

   A `java.util.concurrent.Future` (what milvus-clj returns) goes through
   hive-weave's deref-safe; any other blocking deref (a promise) gets a timed
   `deref`; a plain IDeref such as a test stub's `delay` has no timeout form
   and is dereferenced directly."
  ([op fut] (deref! op fut (timeout-ms)))
  ([op fut ms]
   (let [r (cond
             (instance? java.util.concurrent.Future fut) (ws/deref-safe fut ms ::timeout)
             (instance? clojure.lang.IBlockingDeref fut) (deref fut ms ::timeout)
             :else                                       (deref fut))]
     (if (identical? r ::timeout)
       (throw (ex-info (str "milvus " (name op) " timed out after " ms "ms")
                       {:milvus/timeout true :op op :timeout-ms ms}))
       r))))
