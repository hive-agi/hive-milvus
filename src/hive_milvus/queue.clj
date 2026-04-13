(ns hive-milvus.queue
  "Global in-memory write queue + read fail-soft shim for Milvus outages.

   Single JVM-wide singleton — shared across ALL projects and agents, not
   per-project. Purpose: when the circuit breaker opens because Milvus is
   unreachable, mutating protocol calls are enqueued here instead of
   failing; reads return a degraded response carrying actionable recovery
   tips. On the circuit's :closed transition, a background future drains
   the queue via `hive-weave.parallel/bounded-pmap` with concurrency 8
   and a 15 s per-item timeout. Pre-drain the batch is coalesced by
   (op,id) keeping the latest mutation, so a burst of writes to the same
   entry collapses to one RPC. Any operation that still fails is pushed
   to the tail for the next pass; drain repeats until the queue is empty,
   the circuit reopens, or a pass makes zero progress.

   Intentionally has no hard dependency on the circuit ns or store.clj —
   those wire it in (see `enqueue!`, `drain!`, `degraded-response`,
   `make-store-dispatch`). Keeping the boundary clean lets circuit,
   store, and queue evolve independently and lets tests exercise the
   queue without standing up Milvus."
  (:require [hive-mcp.protocols.memory :as proto]
            [hive-weave.parallel :as parallel]
            [taoensso.timbre :as log])
  (:import [clojure.lang PersistentQueue]))

;; =============================================================================
;; Configuration
;; =============================================================================

(def ^:const max-size
  "Soft cap. Overflow rejects with :queue-full — we'd rather tell the
   caller 'dropping your write' than OOM the JVM. 100k is ~a few hours
   of steady-state writes at a realistic agent cadence."
  100000)

(def ^:const drain-concurrency
  "bounded-pmap worker count. Matches a typical Milvus collection's
   sweet spot for mixed upserts without saturating the gRPC channel."
  8)

(def ^:const drain-timeout-ms
  "Per-item timeout. Generous because upserts do embedding + insert +
   index maintenance; stingy enough that one stuck RPC doesn't wedge
   the drain."
  15000)

;; =============================================================================
;; State
;; =============================================================================

(def ^:private the-queue
  "Process-wide write queue. PersistentQueue gives us structural sharing
   and O(1) conj/peek/pop — important when we're snapshotting under
   contention in `drain-pass!`."
  (atom PersistentQueue/EMPTY))

(def ^:private draining?
  "CAS guard so two circuit transitions can't spin up parallel drains.
   Reset in the `finally` of `drain!`."
  (atom false))

(def ^:private metrics
  "Cumulative counters for observability. Not reset on drain so operators
   can see lifetime queue pressure."
  (atom {:enqueued      0
         :rejected-full 0
         :drained-ok    0
         :drained-fail  0
         :drain-passes  0}))

(defn size
  "Current queue depth (cheap atom read)."
  []
  (count @the-queue))

(defn full?
  "Whether the next enqueue will be rejected. Callers should prefer
   `enqueue!` and check its result — this is for status/metrics."
  []
  (>= (size) max-size))

(defn drain-running?
  "Whether a drain pass is currently in flight."
  []
  @draining?)

(defn stats
  "Snapshot of cumulative metrics + current depth."
  []
  (assoc @metrics :depth (size) :max-size max-size :draining? (drain-running?)))

(defn clear!
  "Drop every queued op. Test helper + escape hatch — never call in
   production without understanding what you're throwing away."
  []
  (reset! the-queue PersistentQueue/EMPTY)
  :cleared)

;; =============================================================================
;; Enqueue
;; =============================================================================

(defn- now-ms [] (System/currentTimeMillis))

(defn enqueue!
  "Append an op descriptor to the queue. An op is a map of shape:

     {:op   :add-entry!      ;; protocol method keyword
      :id   \"entry-uuid\"    ;; nil for singleton ops like :cleanup-expired!
      :args [entry]}          ;; args to splat into the protocol call

   Returns `{:success? true :queued? true :depth N :tips [...]}` on
   success, or `{:success? false :error :queue-full :depth N :tips [...]}`
   when the soft cap is hit. Uses `swap-vals!` so we can atomically
   detect whether the swap added an element — no dropped writes under
   contention, no false overflows."
  [op]
  (let [op*       (assoc op :ts (now-ms))
        [old new] (swap-vals! the-queue
                               (fn [q]
                                 (if (>= (count q) max-size)
                                   q
                                   (conj q op*))))
        accepted? (> (count new) (count old))]
    (if accepted?
      (do (swap! metrics update :enqueued inc)
          {:success? true
           :queued?  true
           :depth    (count new)
           :tips     [(str "Write queued for replay (depth " (count new)
                           "/" max-size ").")
                      "Will drain automatically when Milvus reconnects."]})
      (do (swap! metrics update :rejected-full inc)
          (log/warn "hive-milvus.queue: overflow, rejecting op" (:op op)
                    "depth=" (count new))
          {:success? false
           :error    :queue-full
           :depth    (count new)
           :tips     [(str "Write queue full at " max-size " ops — Milvus still unreachable.")
                      "Operation NOT recorded. Retry after circuit recovers."
                      "Coordinator can force a drain or widen the cap if sustained."]}))))

;; =============================================================================
;; Read fail-soft
;; =============================================================================

(defn degraded-response
  "Build a read fail-soft response map used by store.clj read methods
   when the circuit is open. Carries actionable tips instead of an
   opaque error — callers (agents, MCP tools) can surface retry-after
   hints and the pending-write depth so the human sees what's pending.

   Options:
     :retry-after-ms   Hint for when to retry (default 5000).
     :reconnect-eta-ms Best-guess ETA from circuit state (nil = unknown).
     :operation        Keyword naming the read that degraded, for tips.
     :extra-tips       Extra hints to append after the standard tips."
  ([] (degraded-response {}))
  ([{:keys [retry-after-ms reconnect-eta-ms operation extra-tips]
     :or   {retry-after-ms 5000}}]
   (let [depth (size)]
     {:success?         false
      :degraded?        true
      :backend          "milvus"
      :reconnecting?    true
      :retry-after-ms   retry-after-ms
      :reconnect-eta-ms reconnect-eta-ms
      :queue-depth      depth
      :operation        operation
      :tips             (vec
                          (concat
                            ["Milvus unreachable — returning degraded response (no data)."
                             (format "Retry in ~%.1fs once the circuit recovers."
                                     (/ (double retry-after-ms) 1000.0))
                             (if reconnect-eta-ms
                               (format "Reconnect ETA: ~%.1fs (circuit breaker estimate)."
                                       (/ (double reconnect-eta-ms) 1000.0))
                               "Reconnect ETA unknown — background healing loop is running.")
                             (str "Pending writes in queue: " depth
                                  " (will replay on reconnect).")
                             "For time-critical queries, retry with a shorter timeout or fall back to cached data."]
                            extra-tips))})))

;; =============================================================================
;; Coalesce
;; =============================================================================

(defn- coalesce-key
  "Dedupe key. Ops with :id coalesce per (op, id); ops without an :id
   (e.g. :cleanup-expired!) collapse to a single slot keyed on the op
   name so bulk repeats don't fan out."
  [op]
  (if-let [id (:id op)]
    [(:op op) id]
    [:singleton (:op op)]))

(defn coalesce
  "Reduce a batch of ops to the latest mutation per (op, id), preserving
   first-seen order so the drain respects arrival sequence. Public so
   tests can exercise it directly.

   Example:
     [{:op :add-entry! :id \"a\" :args [{...}]}
      {:op :update-entry! :id \"b\" :args [\"b\" {...}]}
      {:op :add-entry! :id \"a\" :args [{...updated}]}]
     => collapses the two writes to 'a' into the second one, keeps 'b'."
  [ops]
  (let [indexed (map-indexed vector ops)
        first-seen (persistent!
                     (reduce (fn [m [i op]]
                               (let [k (coalesce-key op)]
                                 (if (contains? m k) m (assoc! m k i))))
                             (transient {})
                             indexed))
        latest (persistent!
                 (reduce (fn [m op] (assoc! m (coalesce-key op) op))
                         (transient {})
                         ops))]
    (->> (vals latest)
         (sort-by #(get first-seen (coalesce-key %)))
         vec)))

;; =============================================================================
;; Drain
;; =============================================================================

(defn- run-op
  "Execute one queued op via `dispatch-fn`. Return `::ok` on success,
   `::failed` on any thrown exception. bounded-pmap's timeout path
   returns its own `::failed` fallback, so we converge on the same
   sentinel for retry logic."
  [dispatch-fn op]
  (try
    (dispatch-fn op)
    ::ok
    (catch Throwable e
      (log/warn "hive-milvus.queue drain op failed:" (:op op) "/" (:id op)
                "—" (ex-message e))
      ::failed)))

(defn- drain-pass!
  "One drain iteration:
     1. Atomically swap the queue for EMPTY (snapshot the pending batch).
     2. Coalesce the snapshot by (op,id) keeping latest mutation.
     3. Dispatch via `bounded-pmap` with concurrency/timeout.
     4. Re-enqueue any failed ops at the TAIL for the next pass.

   Returns a summary `{:attempted :succeeded :failed :failed-ops}`."
  [dispatch-fn]
  (let [[snapshot _] (swap-vals! the-queue (constantly PersistentQueue/EMPTY))
        batch        (coalesce (vec snapshot))
        _            (log/info "hive-milvus.queue drain pass: coalesced"
                               (count snapshot) "→" (count batch))
        results      (parallel/bounded-pmap
                       {:concurrency drain-concurrency
                        :timeout-ms  drain-timeout-ms
                        :fallback    ::failed}
                       (partial run-op dispatch-fn)
                       batch)
        failed-ops   (into []
                           (keep (fn [[op r]]
                                   (when (not= r ::ok) op)))
                           (map vector batch results))
        ok-cnt       (- (count batch) (count failed-ops))]
    (when (seq failed-ops)
      ;; Re-queue failures at the tail. New arrivals during the pass
      ;; remain ahead in FIFO — they went straight onto `the-queue`
      ;; after the snapshot CAS.
      (swap! the-queue #(reduce conj % failed-ops)))
    (swap! metrics (fn [m]
                     (-> m
                         (update :drained-ok + ok-cnt)
                         (update :drained-fail + (count failed-ops))
                         (update :drain-passes inc))))
    {:attempted  (count batch)
     :succeeded  ok-cnt
     :failed     (count failed-ops)
     :failed-ops failed-ops}))

(defn drain!
  "Drain the queue until it's empty, the circuit reopens, or a pass
   makes zero forward progress (safety against a flapping backend).
   Intended to be called from a future spawned on the circuit's
   :closed transition — this function runs drain passes inline in the
   caller's thread and will block until one of its stop conditions
   triggers.

   Options (map):
     :dispatch-fn    REQUIRED. (fn [op] ...) that applies one queued op
                     to Milvus. Throw or return falsey on failure so
                     the op is re-queued for the next pass. Typically
                     built via `make-store-dispatch`.
     :circuit-open?  (fn []) predicate. If it returns truthy between
                     passes, drain stops and leaves the rest queued.
                     Default: never-open (best-effort).

   Returns `{:result :drained|:circuit-reopened|:all-failed|:already-running
             :attempted N :succeeded N :failed N :passes N}`."
  [{:keys [dispatch-fn circuit-open?]
    :or   {circuit-open? (constantly false)}}]
  (assert dispatch-fn "drain! requires :dispatch-fn")
  (if-not (compare-and-set! draining? false true)
    {:result :already-running :attempted 0 :succeeded 0 :failed 0 :passes 0}
    (try
      (log/info "hive-milvus.queue drain started — depth" (size))
      (loop [tot {:attempted 0 :succeeded 0 :failed 0 :passes 0}]
        (cond
          (zero? (size))
          (do (log/info "hive-milvus.queue drain complete —" tot)
              (assoc tot :result :drained))

          (circuit-open?)
          (do (log/warn "hive-milvus.queue drain aborted — circuit reopened, depth"
                        (size))
              (assoc tot :result :circuit-reopened))

          :else
          (let [pass (drain-pass! dispatch-fn)
                tot' {:attempted (+ (:attempted tot) (:attempted pass))
                      :succeeded (+ (:succeeded tot) (:succeeded pass))
                      :failed    (+ (:failed tot) (:failed pass))
                      :passes    (inc (:passes tot))}]
            (if (and (pos? (:attempted pass)) (zero? (:succeeded pass)))
              ;; Network still bad — don't spin. Leave failures queued
              ;; for the next circuit transition.
              (do (log/warn "hive-milvus.queue drain pass made zero progress — backing off")
                  (assoc tot' :result :all-failed))
              (recur tot')))))
      (finally
        (reset! draining? false)))))

;; =============================================================================
;; Store integration helper
;; =============================================================================

(defn make-store-dispatch
  "Build a drain `dispatch-fn` bound to a concrete `MilvusMemoryStore`.
   Each queued op becomes a direct `proto/*` call against the store.
   The circuit wiring owns the question of whether these calls go
   through `resilient` (risking re-queueing loops) or a bypass path;
   this helper just dispatches on `:op`. On unknown `:op`, throws —
   the caller's `drain-pass!` catches and logs it as a failure.

   Keeping this in the queue ns (rather than store.clj) means the
   store doesn't need to know the queue exists, and circuit wiring
   can swap in its own dispatch-fn for testing."
  [store]
  (fn dispatch [{:keys [op args]}]
    (case op
      :add-entry!        (proto/add-entry! store (first args))
      :update-entry!     (proto/update-entry! store (first args) (second args))
      :delete-entry!     (proto/delete-entry! store (first args))
      :log-access!       (proto/log-access! store (first args))
      :record-feedback!  (proto/record-feedback! store (first args) (second args))
      :update-staleness! (proto/update-staleness! store (first args) (second args))
      :cleanup-expired!  (proto/cleanup-expired! store)
      (throw (ex-info "hive-milvus.queue: unknown queued op"
                      {:op op :args args})))))
