(ns hive-milvus.store.health
  "Resilient connection + liveness machinery for MilvusMemoryStore.

   Two complementary tracks:

   - REACTIVE (`with-auto-reconnect`, `resilient` macro): wraps each RPC
     body in a try/classify/retry pipeline. Transient gRPC failures kick
     the background heal loop, block up to a budget, and retry once.

   - PROACTIVE (`ensure-live!`, `health-cache`): before every RPC fires,
     checks a ~5s-cached liveness snapshot so healing starts before the
     first post-idle RPC eats an UNAVAILABLE.

   Owns the `reconnect-state` and `health-cache` `defonce` atoms, plus
   the background reconnect loop future."
  (:require [hive-dsl.result :as dsl-r]
            [hive-milvus.circuit :as circuit]
            [hive-milvus.failure :as failure]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]))

;; =========================================================================
;; Background Reconnect Loop
;; =========================================================================

(defonce reconnect-state
  ;; Tracks background reconnect loop. Keys: :running? :future :last-attempt
  (atom {:running? false :future nil :last-attempt nil}))

(defn backoff-ms
  "Exponential backoff with jitter. attempt starts at 1.
   base-ms doubles each attempt, capped at max-ms, plus ±25% jitter so
   multiple clients don't synchronise their retry storms."
  [attempt base-ms max-ms]
  (let [raw     (* base-ms (bit-shift-left 1 (min 20 (dec attempt))))
        capped  (min (long raw) (long max-ms))
        jitter  (long (* capped 0.25 (- (rand) 0.5) 2))]
    (max 100 (+ capped jitter))))

(defn try-reconnect!
  "Single reconnect attempt using stored config. Returns true on success.

   Carries `:transport` through so the reactive heal loop reconnects
   with whichever transport the store was originally configured with —
   essential for the hybrid-transport story (PR-3): the same heal
   machinery serves both `:grpc` (rebuild the channel) and `:http`
   (idempotent re-init of the JDK HttpClient)."
  [config-atom]
  (try
    (let [cfg @config-atom
          milvus-cfg (into {} (filter (comp some? val))
                           (select-keys cfg [:transport :host :port :token
                                             :database :secure]))
          milvus-cfg (merge {:connect-timeout-ms 30000} milvus-cfg)]
      (milvus/connect! milvus-cfg)
      (log/info "Milvus reconnected successfully")
      true)
    (catch Exception e
      (log/debug "Milvus reconnect attempt failed:" (.getMessage e))
      false)))

(defn start-reconnect-loop!
  "Background healing loop: retries forever with exponential backoff + jitter,
   capped at `max-ms`. Runs until the store is reached and `try-reconnect!`
   succeeds, or until another caller flips `:running?` back to false (e.g.
   `disconnect!`). No attempt ceiling — intermittent outages should self-heal
   whenever the network comes back."
  [config-atom & {:keys [base-ms max-ms]
                  :or   {base-ms 1000 max-ms 60000}}]
  (when (compare-and-set! reconnect-state
          (assoc @reconnect-state :running? false)
          (assoc @reconnect-state :running? true))
    (let [fut (future
                (loop [n 1]
                  (when (:running? @reconnect-state)
                    (swap! reconnect-state assoc :last-attempt (System/currentTimeMillis))
                    (if (try-reconnect! config-atom)
                      (do
                        (log/info "Milvus reconnect loop recovered after" n "attempts")
                        (swap! reconnect-state assoc :running? false))
                      (let [wait (backoff-ms n base-ms max-ms)]
                        (log/debug "Milvus reconnect attempt" n "failed, next in" (/ wait 1000.0) "s")
                        (Thread/sleep wait)
                        (recur (inc n)))))))]
      (swap! reconnect-state assoc :future fut))))

(defn await-reconnect!
  "Block up to `max-wait-ms` waiting for the background reconnect loop
   to recover the milvus connection. Returns true if the connection is
   up at the end of the wait (reconnect succeeded or we lucked out),
   false if the deadline passed with the loop still running."
  [max-wait-ms]
  (let [deadline (+ (System/currentTimeMillis) max-wait-ms)]
    (loop []
      (cond
        (milvus/connected?)                       true
        (>= (System/currentTimeMillis) deadline)  (milvus/connected?)
        :else (do (Thread/sleep 200) (recur))))))

(defn kick-reconnect!
  "Drop the dead milvus client and start the background heal loop if
   it isn't already running. Idempotent. Extracted so the reactive path
   and test hooks can call it without duplicating the disconnect + CAS."
  [config-atom]
  ;; Explicitly drop the dead client so `milvus/connected?` reports
  ;; false until the background loop installs a fresh one. Without
  ;; this, `await-reconnect!` returns immediately because the atom
  ;; still holds the broken client.
  (try (milvus/disconnect!) (catch Throwable _ nil))
  (when-not (:running? @reconnect-state)
    (log/info "Starting background reconnect loop")
    (start-reconnect-loop! config-atom)))

;; =========================================================================
;; Reactive Retry Pipeline
;; =========================================================================

(defn attempt-call
  "Run `f` under a try-effect* that tags any Throwable as a raw
   :milvus/call error carrying the original exception. No classification
   yet — classification happens in `r/map-err` so the pipeline stays
   linear."
  [f]
  (try (dsl-r/ok (f))
       (catch Throwable e
         (dsl-r/err :milvus/call {:throwable e}))))

(defn classify-err
  "Called by `map-err`: lift the raw `{:throwable e}` error into a
   MilvusFailure ADT. Re-throws non-connection failures so callers
   outside the resilient retry path keep seeing exceptions for genuine
   programming errors (the original contract of with-auto-reconnect)."
  [err]
  (let [t       (:throwable err)
        failure (failure/classify t)]
    (if (failure/transient? failure)
      (dsl-r/err :milvus/failure {:failure failure})
      ;; Fatal: re-throw so host code sees the real stack trace.
      (throw t))))

(defn retry-once
  "Second-chance step for the pipeline. On a transient failure, kick
   the background heal loop, block up to `budget-ms` for recovery, then
   retry `f` one time. On recovery failure or second-attempt failure we
   carry the failure forward through the Result chain. Never throws for
   transient paths — fatal exceptions already unwound in `classify-err`."
  [config-atom f budget-ms]
  (fn [err]
    (let [failure (:failure err)
          msg     (:message failure)]
      (circuit/record-failure!)
      (log/warn "Milvus operation failed (transient):" msg)
      (kick-reconnect! config-atom)
      (if (await-reconnect! budget-ms)
        ;; Recovery path: retry exactly once.
        (let [retried (attempt-call f)]
          (if (dsl-r/ok? retried)
            (do (circuit/record-success!) retried)
            (dsl-r/err :milvus/failure
                       {:failure (failure/classify
                                   (:throwable retried))})))
        ;; Budget exhausted before reconnect completed.
        (do (log/warn "Milvus reconnect did not complete within budget"
                      budget-ms "ms")
            (dsl-r/err :milvus/failure
                       {:failure (failure/reconnect-timeout msg)}))))))

(defn with-auto-reconnect
  "Execute `f` with transparent reconnect-and-retry on transient gRPC/
   connection failures. Flat `hive-dsl.result` pipeline:

     attempt -> classify -> (maybe) retry -> legacy-shape

   Semantics preserved from the old nested-try/catch implementation:

   - Circuit breaker gated first: if :open and in cooldown we return the
     breaker's fail map without running `f`.
   - Fatal exceptions (non-transient) are re-thrown — callers outside
     this helper keep their existing contract.
   - Transient failures kick the background reconnect loop, block up to
     `budget-ms` for recovery, then retry `f` exactly once.
   - Final failure (second attempt, or reconnect timeout) is translated
     back to the legacy `{:success? false :errors [...] :reconnecting? ...}`
     map so protocol callers continue to see the shape they expect.

   Why inline reactive retry: tailscale userspace netstack (and any
   NAT-style intermediary) can drop an idle gRPC flow between RPCs.
   Returning the raw failure to the caller forces every caller to
   implement retry, which they don't."
  ([config-atom f]
   (with-auto-reconnect config-atom f 8000))
  ([config-atom f budget-ms]
   (if-let [blocked (circuit/check)]
     blocked
     (let [result (-> (attempt-call f)
                      (dsl-r/map-err classify-err)
                      (dsl-r/bind (fn [v]
                                    (circuit/record-success!)
                                    (dsl-r/ok v)))
                      (dsl-r/map-err (retry-once config-atom f budget-ms)))]
       (cond
         (dsl-r/ok? result)  (:ok result)
         (dsl-r/err? result) (failure/->legacy-map (:failure result))
         :else               result)))))

;; -------------------------------------------------------------------------
;; Preemptive liveness check (ensure-live!)
;; -------------------------------------------------------------------------
;;
;; Why: `with-auto-reconnect` is REACTIVE — the first post-idle RPC eats an
;; UNAVAILABLE, triggers the reconnect loop, then blocks up to 8s inside
;; await-reconnect! before retrying. `ensure-live!` is PROACTIVE — before
;; the RPC fires, check whether the client is alive; if not, kick the
;; background loop now so healing overlaps the caller's preparation latency.
;;
;; Cache: milvus/connected? is a cheap atom deref in milvus-clj, but we
;; still cache the result ~5s so a burst of RPCs doesn't repeatedly branch
;; on it. The cache is a process-wide atom — mirrors the singleton nature
;; of the milvus-clj client.

(defonce health-cache
  ;; Cached liveness snapshot. :ts = System/currentTimeMillis of last check,
  ;; :alive? = milvus/connected? reading at that time.
  (atom {:ts 0 :alive? false}))

(def ^:private health-cache-ttl-ms
  "Positive liveness stays valid this long before re-verification.
   5s: short enough that a dropped connection is noticed before a
   typical multi-RPC request completes, long enough that a burst of
   RPCs amortises to a single check."
  5000)

(defn kick-reconnect-now!
  "Preemptive sibling of the `kick-reconnect!` letfn inside
   `with-auto-reconnect`. Drops the dead client so milvus/connected?
   stops lying, then starts the background reconnect loop if not
   already running. Idempotent."
  [config-atom]
  (try (milvus/disconnect!) (catch Throwable _ nil))
  (when-not (:running? @reconnect-state)
    (log/info "ensure-live!: starting background reconnect loop preemptively")
    (start-reconnect-loop! config-atom)))

(defn ensure-live!
  "Preemptive liveness gate. Called at the top of every `resilient` body.
   Reads a ~5s cached liveness snapshot; on cache miss re-verifies via
   milvus/connected?. If the client is dead OR the cache is stale (>5s)
   and the re-check returns false, kicks the background reconnect loop
   so healing starts before the caller's RPC fails.

   Does NOT block waiting for recovery — `with-auto-reconnect` still
   owns the reactive retry + await-reconnect! budget. This just shrinks
   the window between 'connection died' and 'reconnect loop running'.

   Returns true if the client is believed live at the end of the call,
   false if still dead (the caller's RPC will then hit the reactive path)."
  [config-atom]
  (let [now   (System/currentTimeMillis)
        cache @health-cache]
    (if (and (:alive? cache)
             (< (- now (:ts cache)) health-cache-ttl-ms))
      true
      (let [alive? (try (milvus/connected?) (catch Throwable _ false))]
        (reset! health-cache {:ts now :alive? alive?})
        (when-not alive?
          (kick-reconnect-now! config-atom))
        alive?))))

(defn invalidate-health-cache!
  "Force the next ensure-live! to re-verify. Hook for the reactive
   retry path to call after observing a failure — keeps the preemptive
   cache honest once a reactive heal fires."
  []
  (swap! health-cache assoc :ts 0 :alive? false))

(defmacro resilient
  "Wrap `body` in `with-auto-reconnect`: on transient gRPC/connection
   failure (UNAVAILABLE, DEADLINE_EXCEEDED, \"Keepalive failed\", etc.)
   the background reconnect loop is kicked, we block briefly for it to
   install a fresh client, and `body` is re-executed once. Transparent
   to callers — they either see the successful retry result or a graceful
   {:success? false :reconnecting? true} map after the budget is spent.

   Before executing `body`, calls `ensure-live!` to proactively detect
   a dead client and start the reconnect loop early. The reactive
   with-auto-reconnect path still handles any failure that slips through.

   Intended for use inside MilvusMemoryStore protocol method bodies where
   `config-atom` is captured from the defrecord."
  [config-atom & body]
  `(do
     (hive-milvus.store.health/ensure-live! ~config-atom)
     (hive-milvus.store.health/with-auto-reconnect
       ~config-atom (fn [] ~@body))))
