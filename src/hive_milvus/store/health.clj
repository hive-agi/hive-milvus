(ns hive-milvus.store.health
  "DEPRECATED — kept only as a re-export shim for the public surface that
   pre-resilience-split consumers (`store/batch.clj`, `store/entries.clj`,
   `store/lifecycle.clj`, `store/analytics.clj`, `store/staleness.clj`)
   already `:refer` from this ns.

   New code MUST require directly from one of:

     hive-milvus.resilience.probe      — cached liveness query
     hive-milvus.resilience.reconnect  — heal-loop + verified reconnect
     hive-milvus.resilience.retry      — reactive retry + `resilient` macro

   Why split: SRP. The old monolith owned 5 concerns (circuit, probe,
   loop, retry, classify) in one 290-line file; each had its own rate
   of change and test surface. The split makes adding a transport (e.g.
   future WebSocket) require zero changes to the resilience layer —
   only an `extend-type` for `milvus-clj.client/ILivenessProbe`.

   The bug fix that motivated the split: the old loop's success
   criterion was post-`connect!` atom state, not an actual probe
   round-trip. After a network flip the atom would be set fresh while
   the underlying transport was still unreachable, so the loop exited
   prematurely and operators had to restart the JVM. The new
   `reconnect/try-reconnect-and-verify!` requires both `connect!` AND
   a successful `probe!` to declare recovery.

   This shim re-exports the public surface so old consumers compile
   unchanged. Will be deleted in a future PR after consumers migrate."
  (:require [hive-milvus.resilience.probe    :as probe]
            [hive-milvus.resilience.reconnect :as reconnect]
            [hive-milvus.resilience.retry    :as retry]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

;; -------------------------------------------------------------------------
;; State (atoms — point at the new locations so :reset! / @ deref reads
;; mirror what the resilience layer uses)
;; -------------------------------------------------------------------------

(def reconnect-state reconnect/reconnect-state)

(def health-cache
  ;; Legacy diagnostic surface only. The active liveness cache lives
  ;; inside `milvus-clj.api` (keyed off the singleton client). This
  ;; atom is never read by the resilience layer; it stays here so any
  ;; legacy code that destructures `@health-cache` does not crash.
  (atom {:ts 0 :alive? false}))

;; -------------------------------------------------------------------------
;; Reactive pipeline — re-exports
;; -------------------------------------------------------------------------

(def attempt-call         retry/attempt-call)
(def classify-err         retry/classify-err)
(def retry-once           retry/retry-once)
(def with-auto-reconnect  retry/with-auto-reconnect)

;; Macros can't be `def`-aliased — they need fresh defmacros that
;; expand into calls on the new ns.

(defmacro resilient
  "DEPRECATED alias for `hive-milvus.resilience.retry/resilient`."
  [config-atom & body]
  `(hive-milvus.resilience.retry/resilient ~config-atom ~@body))

;; -------------------------------------------------------------------------
;; Probe / reconnect — re-exports
;; -------------------------------------------------------------------------

(def ensure-live!             retry/ensure-live!)
(def invalidate-health-cache! probe/invalidate!)
(def start-reconnect-loop!    reconnect/start-loop!)
(def await-reconnect!         reconnect/await!)
(def kick-reconnect!          reconnect/kick!)
(def kick-reconnect-now!      reconnect/kick!)

(defn try-reconnect!
  "DEPRECATED. Redirects to `reconnect/kick!` + `reconnect/await!` so
   callers get probe-verified recovery semantics instead of the old
   atom-state-only success criterion."
  [config-atom]
  (reconnect/kick! config-atom)
  (reconnect/await! 8000))

(defn backoff-ms
  "DEPRECATED. The reconnect loop now owns its own backoff curve (private
   to `resilience.reconnect`). Kept here only for any external caller
   that pinned to the old shape; signature preserved."
  [attempt base-ms max-ms]
  (let [raw    (* base-ms (bit-shift-left 1 (min 20 (dec attempt))))
        capped (min (long raw) (long max-ms))
        jitter (long (* capped 0.25 (- (rand) 0.5) 2))]
    (max 100 (+ capped jitter))))
