(ns hive-milvus.resilience.scheduler
  "Proactive periodic liveness scheduler — Boundary stage of the resilience
   CPPB pipeline.

   Single responsibility: own a timer that fires a health check on a fixed
   cadence, so a connection that dies while the store is IDLE (no traffic to
   trigger the reactive `retry` path or the on-demand `ensure-live!` gate)
   still heals. This closes the gap where auto-heal was 100% traffic-driven:
   after a network blip with no in-flight RPC, nothing re-probed and the dead
   client sat dead until the next operation happened to touch it.

   Why this ns owns ONLY the timer (SRP + DRY):
   - The health *decision* — 'is it dead, and if so start a heal loop WITHOUT
     disturbing one already in flight' — already lives in `retry/ensure-live!`
     (reads cached liveness, kicks the reconnect loop only `when-not` it is
     already running). Re-implementing it here would duplicate the guard and
     risk calling `reconnect/kick!` mid-heal, which drops the fresh client on
     every tick. So the tick DELEGATES the policy and keeps only the
     scheduling (Boundary) concern.
   - Complements, does not replace: reactive retry (on failure) + preemptive
     ensure-live! (at op start) stay as-is; this adds the idle heartbeat that
     neither covered.

   Lifecycle: started from `store.lifecycle/connect!` once the config-atom
   exists; stopped from `store.lifecycle/disconnect!` before teardown, so the
   timer can't kick a heal loop right after the operator closed the store.

   Time discipline: never call `System/currentTimeMillis` directly — go
   through `hive-ttracking.clock/now-millis` (mirrors probe/reconnect)."
  (:require [hive-dsl.result :as r]
            [hive-milvus.resilience.probe :as probe]
            [hive-milvus.resilience.retry :as retry]
            [hive-ttracking.clock :as clock]
            [taoensso.timbre :as log]
            [malli.core :as m])
  (:import [java.util.concurrent Executors ScheduledExecutorService ThreadFactory TimeUnit]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

;; =============================================================================
;; State (Collect) — one atom, mirrors reconnect-state's shape discipline
;; =============================================================================

(def ^:const default-interval-s
  "Idle liveness-probe cadence (seconds). 30 s sits well above the ~5 s probe
   cache TTL (so every tick forces a genuine round-trip, never a stale
   positive) and well below any human-scale idle window (so a blip heals
   within one tick). Promotable to a `MilvusConfig` field if per-deployment
   tuning is ever needed."
  30)

(defonce ^:private scheduler-state
  ;; :executor    ScheduledExecutorService — the timer (for shutdown)
  ;; :running?    bool  — is the timer active?
  ;; :config-atom atom  — captured at start!, threaded into the health decision
  ;; :interval-s  long  — active cadence
  ;; :tick-count  long  — ticks fired since start
  ;; :last-tick   long  — wall-clock millis of last tick
  ;; :last-alive? bool  — liveness result of the last tick
  (atom {:executor nil :running? false :config-atom nil
         :interval-s nil :tick-count 0 :last-tick nil :last-alive? nil}))

;; =============================================================================
;; Health tick (Pipeline) — delegate the policy, own only the cadence
;; =============================================================================

(defn health-tick!
  "One idle health check. Forces a fresh liveness read (invalidate the probe
   cache so `ensure-live!`'s cached `alive?` re-probes rather than trusting a
   possibly stale-positive snapshot), then delegates to `retry/ensure-live!`,
   which kicks the background reconnect loop iff the client is dead AND no
   loop is already running. Returns the boolean liveness at end of tick.

   `r/rescue` guarantees a boolean, never a throw — a tick must never be the
   thing that kills its own scheduler."
  [config-atom]
  (r/rescue false
    (do
      (probe/invalidate!)
      (retry/ensure-live! config-atom))))

;; =============================================================================
;; Scheduler lifecycle (Boundary)
;; =============================================================================

(defn- make-tick-task
  "Runnable that runs one health tick and records it. MUST catch Throwable:
   a ScheduledExecutorService silently stops scheduling ALL future runs if a
   task throws — the healer must never disable itself."
  [config-atom]
  (reify Runnable
    (run [_]
      (try
        (let [alive? (health-tick! config-atom)]
          (swap! scheduler-state
                 (fn [s] (-> s
                             (update :tick-count inc)
                             (assoc :last-tick (clock/now-millis)
                                    :last-alive? alive?))))
          (when-not alive?
            (log/warn "milvus health scheduler: tick found dead client; reconnect loop kicked")))
        (catch Throwable t
          (log/error t "milvus health scheduler: tick threw (caught at boundary)"))))))

(defn start!
  "Start the periodic idle-liveness scheduler. Idempotent (a second call while
   running is a no-op). `interval-s <= 0` disables it. The first tick fires
   after one full interval — connect! has already established (or handed off)
   the connection, so there is no point probing immediately."
  ([config-atom] (start! config-atom default-interval-s))
  ([config-atom interval-s]
   (cond
     (or (nil? interval-s) (<= interval-s 0))
     (do (log/info "milvus health scheduler: disabled (interval-s" interval-s ")")
         {:started false :reason "disabled"})

     (:running? @scheduler-state)
     {:started false :reason "already-running"}

     :else
     (try
       (let [^ScheduledExecutorService executor
             (Executors/newSingleThreadScheduledExecutor
              (reify ThreadFactory
                (newThread [_ runnable]
                  (doto (Thread. runnable "hive-milvus-health")
                    (.setDaemon true)))))]
         (.scheduleWithFixedDelay executor
                                  (make-tick-task config-atom)
                                  (long interval-s)
                                  (long interval-s)
                                  TimeUnit/SECONDS)
         (swap! scheduler-state assoc
                :executor executor :running? true
                :config-atom config-atom :interval-s (long interval-s))
         (log/info "milvus health scheduler: started" {:interval-s interval-s})
         {:started true :interval-s interval-s})
       (catch Exception e
         (log/error e "milvus health scheduler: failed to start")
         {:started false :reason (.getMessage e)})))))

(defn stop!
  "Stop the scheduler (if running). Called from the store's disconnect! path so
   the timer can't kick a heal loop after a deliberate close. Idempotent."
  []
  (if-not (:running? @scheduler-state)
    {:stopped false :reason "not-running"}
    (let [^ScheduledExecutorService executor (:executor @scheduler-state)
          ticks (:tick-count @scheduler-state)]
      (try
        (.shutdownNow executor)
        (catch Exception e
          (log/warn e "milvus health scheduler: error during shutdown")))
      (swap! scheduler-state assoc :executor nil :running? false :config-atom nil)
      (log/info "milvus health scheduler: stopped after" ticks "ticks")
      {:stopped true :ticks-completed ticks})))

(defn status
  "Current scheduler status — for ops tooling / health endpoints."
  []
  (let [s @scheduler-state]
    {:running?    (:running? s)
     :interval-s  (:interval-s s)
     :tick-count  (:tick-count s)
     :last-tick   (:last-tick s)
     :last-alive? (:last-alive? s)}))

(def ConfigAtom
  "Mutable store-config holder threaded into the health-check policy."
  [:fn #(instance? clojure.lang.IAtom %)])

(def StartResult
  "Return shape of `start!`."
  [:map {:closed true}
   [:started :boolean]
   [:reason {:optional true} [:maybe :string]]
   [:interval-s {:optional true} number?]])

(def StopResult
  "Return shape of `stop!`."
  [:map {:closed true}
   [:stopped :boolean]
   [:reason {:optional true} :string]
   [:ticks-completed {:optional true} :int]])

(def SchedulerStatus
  "Return shape of `status` — snapshot of the scheduler state."
  [:map {:closed true}
   [:running? :boolean]
   [:interval-s [:maybe :int]]
   [:tick-count :int]
   [:last-tick [:maybe :int]]
   [:last-alive? [:maybe :boolean]]])

(m/=> health-tick! [:=> [:cat ConfigAtom] :boolean])

(m/=> start!
      [:function
       [:=> [:cat ConfigAtom] StartResult]
       [:=> [:cat ConfigAtom [:maybe number?]] StartResult]])

(m/=> stop! [:=> [:cat] StopResult])

(m/=> status [:=> [:cat] SchedulerStatus])
