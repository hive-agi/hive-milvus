(ns hive-milvus.resilience.retry
  "Reactive try / classify / retry pipeline — Pipeline stage of the
   resilience CPPB pipeline.

   Single responsibility: wrap one RPC body in a fault-tolerant
   pipeline that classifies failure, kicks the reconnect loop, awaits
   verified recovery, and retries exactly once. Public surface:

     - `with-auto-reconnect` — function form, takes a thunk
     - `resilient` — macro sugar that combines `ensure-live!` with
       `with-auto-reconnect` for use inside protocol method bodies

   Composes Promote (`failure/classify`, `circuit/check`) over Boundary
   (`probe`, `reconnect`). Stays pure with respect to the singleton
   client — never mutates `default-client` directly; only orchestrates
   the Boundary nses that do.

   Migration note: this ns replaces `hive-milvus.store.health`. The old
   ns kept circuit, probe, reconnect, classify, retry all in one file
   (5 responsibilities). This split means each concern can evolve
   independently."
  (:require [hive-dsl.result :as dsl-r]
            [hive-milvus.circuit :as circuit]
            [hive-milvus.failure :as failure]
            [hive-milvus.resilience.probe :as probe]
            [hive-milvus.resilience.reconnect :as reconnect]
            [taoensso.timbre :as log]
            [malli.core :as m]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

;; =========================================================================
;; Reactive Retry Pipeline (Promote + Pipeline)
;; =========================================================================

(defn attempt-call
  "Run `f` under a try-effect that tags any Throwable as a raw
   :milvus/call error carrying the original exception. No classification
   yet — classification happens in `r/map-err` so the pipeline stays
   linear.

   Uses bare try/catch (not `try-effect*`) because the carried payload
   is the Throwable itself, not just its message — `classify-err`
   downstream needs the live exception to dispatch on transport class."
  [f]
  (try (dsl-r/ok (f))
       (catch Throwable e
         (dsl-r/err :milvus/call {:throwable e}))))

(defn classify-err
  "Promote stage — pure. Lift a raw `{:throwable e}` error into a
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
  "Pipeline stage — composes Promote (classify) over Boundary
   (`reconnect/kick!`, `reconnect/await!`).

   On a transient failure: kick the background heal loop, block up to
   `budget-ms` for verified recovery (probe round-trip), then retry `f`
   one time. On recovery failure or second-attempt failure we carry the
   failure forward through the Result chain. Never throws for transient
   paths — fatal exceptions already unwound in `classify-err`."
  [config-atom f budget-ms]
  (fn [err]
    (let [failure (:failure err)
          msg     (:message failure)]
      (circuit/record-failure!)
      (log/warn "milvus operation failed (transient):" msg)
      (reconnect/kick! config-atom)
      (if (reconnect/await! budget-ms)
        ;; Recovery path: retry exactly once.
        (let [retried (attempt-call f)]
          (if (dsl-r/ok? retried)
            (do (circuit/record-success!) retried)
            (dsl-r/err :milvus/failure
                       {:failure (failure/classify
                                   (:throwable retried))})))
        ;; Budget exhausted before reconnect verified.
        (do (log/warn "milvus reconnect did not verify within budget"
                      budget-ms "ms")
            (dsl-r/err :milvus/failure
                       {:failure (failure/reconnect-timeout msg)}))))))

(defn with-auto-reconnect
  "Execute `f` with transparent reconnect-and-retry on transient
   connection failures. Flat `hive-dsl.result` pipeline:

     attempt -> classify -> (maybe) retry -> legacy-shape

   Semantics:

   - Circuit breaker gated first: if :open and in cooldown we return
     the breaker's fail map without running `f`.
   - Fatal exceptions (non-transient) are re-thrown — callers outside
     this helper keep their existing contract.
   - Transient failures kick the background reconnect loop, block up
     to `budget-ms` for VERIFIED recovery (probe round-trip), then
     retry `f` exactly once.
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
;; Preemptive liveness gate
;; -------------------------------------------------------------------------

(defn ensure-live!
  "Preemptive liveness gate. Called at the top of every `resilient` body.

   Reads `probe/alive?` (cached). If the cache says dead AND the
   reconnect loop isn't already running, kicks the loop preemptively
   so healing starts before the caller's RPC fails.

   Does NOT block waiting for recovery — `with-auto-reconnect` still
   owns the reactive retry budget. This just shrinks the window between
   'connection died' and 'reconnect loop running'.

   Returns true if probe says alive at the end of the call, false if
   still dead (the caller's RPC will then hit the reactive path)."
  [config-atom]
  (if (probe/alive?)
    true
    (do
      (when-not (:running? @reconnect/reconnect-state)
        (log/info "ensure-live!: starting background reconnect loop preemptively")
        (reconnect/kick! config-atom))
      false)))

(defmacro resilient
  "Wrap `body` in `with-auto-reconnect`: on transient gRPC/HTTP/connection
   failure (UNAVAILABLE, DEADLINE_EXCEEDED, IO timeout, \"Keepalive failed\",
   etc.) the background reconnect loop is kicked, we block briefly for
   it to verify a fresh client via probe round-trip, and `body` is
   re-executed once. Transparent to callers — they either see the
   successful retry result or a graceful
   `{:success? false :reconnecting? true}` map after the budget is spent.

   Before executing `body`, calls `ensure-live!` to proactively detect
   a dead client and start the reconnect loop early. The reactive
   `with-auto-reconnect` path still handles any failure that slips
   through.

   Intended for use inside MilvusMemoryStore protocol method bodies where
   `config-atom` is captured from the defrecord."
  [config-atom & body]
  `(do
     (hive-milvus.resilience.retry/ensure-live! ~config-atom)
     (hive-milvus.resilience.retry/with-auto-reconnect
       ~config-atom (fn [] ~@body))))

(def ConfigAtom
  "Milvus client config atom, passed through to the reconnect Boundary."
  [:fn #(instance? clojure.lang.IAtom %)])

(def Thunk
  "Zero-argument callable wrapping one Milvus RPC."
  [:=> [:cat] :any])

(def MilvusFailure
  "hive-milvus.failure ADT value: closed sum of Milvus failure modes."
  [:map {:closed true}
   [:adt/type [:= :MilvusFailure]]
   [:adt/variant [:enum :milvus/transient :milvus/fatal :milvus/reconnect-timeout]]
   [:message :string]])

(def OkResult
  "hive-dsl.result success Result carrying the RPC return value."
  [:map [:ok :any]])

(def RawCallError
  "Unclassified failure Result carrying the live Throwable."
  [:map
   [:error [:= :milvus/call]]
   [:throwable [:fn #(instance? Throwable %)]]])

(def FailureError
  "Classified failure Result carrying a MilvusFailure ADT value."
  [:map
   [:error [:= :milvus/failure]]
   [:failure MilvusFailure]])

(m/=> attempt-call [:=> [:cat Thunk] [:or OkResult RawCallError]])

(m/=> classify-err [:=> [:cat RawCallError] FailureError])

(m/=> retry-once
      [:=> [:cat ConfigAtom Thunk pos-int?]
       [:=> [:cat FailureError] [:or OkResult FailureError]]])

(m/=> with-auto-reconnect
      [:function
       [:=> [:cat ConfigAtom Thunk] :any]
       [:=> [:cat ConfigAtom Thunk pos-int?] :any]])

(m/=> ensure-live! [:=> [:cat ConfigAtom] :boolean])
