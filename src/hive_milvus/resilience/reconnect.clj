(ns hive-milvus.resilience.reconnect
  "Reconnect-loop ownership — Boundary stage of the resilience CPPB
   pipeline.

   Single responsibility: drive the singleton milvus client back to a
   reachable state after a transient failure. The loop's success
   criterion is an actual probe round-trip, NOT the post-`connect!`
   atom state — that distinction is the bug fix vs the prior design
   where `connect!` always 'succeeded' (it just sets the atom) and the
   loop exited even when the network was still down.

   Decoupled from probe.clj (which owns the cache) and retry.clj
   (which owns the reactive pipeline) per SRP. This ns owns ONE atom
   (`reconnect-state`) and its loop future.

   Time discipline: never call `System/currentTimeMillis` directly — go
   through `hive-ttracking.clock/now-millis` so tests can pin time."
  (:require [hive-dsl.result :as r]
            [hive-milvus.resilience.probe :as probe]
            [hive-ttracking.clock :as clock]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defonce reconnect-state
  ;; Tracks background reconnect loop state.
  ;; :running?     bool   — is the loop currently active?
  ;; :future       Future — the loop's future (for shutdown)
  ;; :last-attempt long   — wall-clock of last try-reconnect-and-verify!
  ;; :attempt      long   — current attempt counter (for backoff)
  (atom {:running? false :future nil :last-attempt nil :attempt 0}))

(defn- backoff-ms
  "Exponential backoff with ±25% jitter. Capped at max-ms so a long
   outage never sleeps for absurd durations. Multiple clients picking
   independent jitter avoids thundering herd."
  [attempt base-ms max-ms]
  (let [raw    (* base-ms (bit-shift-left 1 (min 20 (dec attempt))))
        capped (min (long raw) (long max-ms))
        jitter (long (* capped 0.25 (- (rand) 0.5) 2))]
    (max 100 (+ capped jitter))))

(defn- try-reconnect-and-verify!
  "ONE reconnect attempt. Two-phase contract:
     1. `(milvus/connect! cfg)` — installs a fresh transport client
     2. `(probe/probe-once!)`   — verifies the new client can reach the
                                   server with an actual RPC

   Returns true ONLY if both phases succeed. This is the bug fix: prior
   design treated phase-1 success as recovery, but `connect!` cannot
   detect a server that is up at TCP-level but unreachable at L4/L7
   (k8s pod restart with stale socket pool, transient ingress flap, etc).
   The probe round-trip is the only reliable signal.

   `r/rescue` swallows any throwable into a clean false — the loop never
   propagates exceptions, just retries on the next backoff tick."
  [config-atom]
  (r/rescue false
    (let [cfg        @config-atom
          milvus-cfg (into {} (filter (comp some? val))
                           (select-keys cfg [:transport :host :port :token
                                             :database :secure]))
          milvus-cfg (merge {:connect-timeout-ms 30000} milvus-cfg)]
      (milvus/connect! milvus-cfg)
      (if (probe/probe-once!)
        (do (log/info "milvus reconnect verified by probe")
            true)
        (do (log/debug "milvus reconnect: connect! ok but probe failed; will retry")
            false)))))

(defn start-loop!
  "Background heal loop. Retries forever with exponential backoff +
   jitter, capped at `max-ms`. Exits only when:
     - `try-reconnect-and-verify!` returns true (real round-trip ok), OR
     - another caller flips `:running?` false (e.g. on shutdown).

   No attempt ceiling — intermittent outages should self-heal whenever
   the network comes back."
  [config-atom & {:keys [base-ms max-ms]
                  :or   {base-ms 1000 max-ms 60000}}]
  (when (compare-and-set! reconnect-state
          (assoc @reconnect-state :running? false)
          (assoc @reconnect-state :running? true :attempt 0))
    (let [fut (future
                (loop [n 1]
                  (when (:running? @reconnect-state)
                    (swap! reconnect-state assoc
                           :last-attempt (clock/now-millis)
                           :attempt n)
                    (if (try-reconnect-and-verify! config-atom)
                      (do
                        (log/info "milvus reconnect loop recovered after" n "attempt(s)")
                        (swap! reconnect-state assoc :running? false :attempt 0))
                      (let [wait (backoff-ms n base-ms max-ms)]
                        (log/debug "milvus reconnect attempt" n
                                   "failed, next in" (/ wait 1000.0) "s")
                        (Thread/sleep wait)
                        (recur (inc n)))))))]
      (swap! reconnect-state assoc :future fut))))

(defn await!
  "Block up to `max-wait-ms` for the loop to verify recovery. Returns
   true if the probe says alive at the end, false otherwise.

   Uses `probe/alive?` (cached) so a burst of awaiters doesn't pound the
   server. The loop itself uses `probe/probe-once!` (uncached) inside
   `try-reconnect-and-verify!` to make sure each attempt actually
   round-trips."
  [max-wait-ms]
  (let [deadline (+ (clock/now-millis) max-wait-ms)]
    (loop []
      (cond
        (probe/alive?)                            true
        (>= (clock/now-millis) deadline)          (probe/alive?)
        :else (do (Thread/sleep 200) (recur))))))

(defn kick!
  "Drop the dead client and start the heal loop if it isn't already
   running. Idempotent. Called by:
     - the reactive retry path (after observing a failure)
     - the proactive `ensure-live!` path (when the cache snapshot
       reports dead but the loop is dormant)
     - tests + ops tooling."
  [config-atom]
  ;; Explicitly drop the dead client + invalidate the probe cache so
  ;; `probe/alive?` reports the truth until the background loop installs
  ;; and verifies a fresh client. `r/rescue` swallows any disconnect
  ;; throw — we only care that the slot is cleared.
  (r/rescue nil (milvus/disconnect!))
  (probe/invalidate!)
  (when-not (:running? @reconnect-state)
    (log/info "milvus: starting background reconnect loop")
    (start-loop! config-atom)))

(defn stop!
  "Stop the loop (if running) without disconnecting the client. Used by
   the store's `disconnect!` path so the loop doesn't keep racing to
   reconnect after the operator deliberately closed the store."
  []
  (swap! reconnect-state assoc :running? false))
