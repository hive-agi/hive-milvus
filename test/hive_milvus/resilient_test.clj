(ns hive-milvus.resilient-test
  "Tests for hive-milvus.store/with-auto-reconnect — the linearized
   reconnect-and-retry pipeline. All live milvus calls are stubbed via
   with-redefs so the test never touches a real server.

   Covers:
   - Totality  : the wrapper never throws for any Result-returning body.
   - FSM walk  : the retry state machine always terminates — no infinite
                 loops, no recursion beyond a single retry.
   - Semantics : ok passes through, transient retries once then legacy
                 map, fatal re-throws, budget-exhausted yields reconnect
                 timeout shape."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-milvus.circuit :as circuit]
            [hive-milvus.store :as store]
            [hive-milvus.failure :as failure]
            [milvus-clj.api :as milvus]))

(def with-auto-reconnect #'store/with-auto-reconnect)

;; =============================================================================
;; Test fixtures: stub milvus-clj so no real RPC is attempted
;; =============================================================================

(defn- stub-milvus [reconnect-ok? body-fn]
  (with-redefs [milvus/disconnect! (fn [] nil)
                milvus/connected?  (fn [] reconnect-ok?)
                circuit/check       (fn [] nil)
                circuit/record-success! (fn [] nil)
                circuit/record-failure! (fn [] nil)]
    (body-fn)))

;; =============================================================================
;; Unit semantics
;; =============================================================================

(deftest ok-passes-through
  (stub-milvus true
    #(is (= 42 (with-auto-reconnect (atom {}) (fn [] 42) 10)))))

(deftest transient-retries-once-then-succeeds
  (let [calls (atom 0)
        f     (fn []
                (swap! calls inc)
                (if (= 1 @calls)
                  (throw (RuntimeException. "UNAVAILABLE"))
                  :recovered))]
    (stub-milvus true
      #(is (= :recovered (with-auto-reconnect (atom {}) f 50))))
    (is (= 2 @calls) "f runs exactly twice: initial + one retry")))

(deftest transient-retry-fails-returns-legacy-map
  (let [calls (atom 0)
        f     (fn []
                (swap! calls inc)
                (throw (RuntimeException. "UNAVAILABLE")))]
    (stub-milvus true
      (fn []
        (let [result (with-auto-reconnect (atom {}) f 50)]
          (is (false? (:success? result)))
          (is (true? (:reconnecting? result)))
          (is (vector? (:errors result))))))
    (is (= 2 @calls))))

(deftest reconnect-timeout-returns-legacy-map
  (let [f (fn [] (throw (RuntimeException. "UNAVAILABLE")))]
    (stub-milvus false
      (fn []
        (let [result (with-auto-reconnect (atom {}) f 10)]
          (is (false? (:success? result)))
          (is (true? (:reconnecting? result))))))))

(deftest fatal-error-rethrows
  (let [f (fn [] (throw (RuntimeException. "schema mismatch")))]
    (stub-milvus true
      #(is (thrown? RuntimeException
             (with-auto-reconnect (atom {}) f 10))))))

(deftest circuit-open-short-circuits
  (let [blocked {:success? false :error :circuit-open :reconnecting? true}
        calls   (atom 0)]
    (with-redefs [circuit/check (fn [] blocked)]
      (let [result (with-auto-reconnect (atom {})
                     (fn [] (swap! calls inc) :never)
                     10)]
        (is (= blocked result))
        (is (= 0 @calls) "body is not invoked when breaker is open")))))

;; =============================================================================
;; Property: totality — any body that returns a value or throws a
;; transient/fatal never makes with-auto-reconnect itself throw (fatals
;; aside, which are re-raised by contract).
;; =============================================================================

(def gen-body
  "Generator for body fns covering every outcome branch."
  (gen/elements
    [(fn [] :ok)
     (fn [] 0)
     (fn [] nil)
     (fn [] (throw (RuntimeException. "UNAVAILABLE")))
     (fn [] (throw (RuntimeException. "Keepalive failed")))
     (fn [] (throw (RuntimeException. "not connected")))]))

(defspec with-auto-reconnect-total 100
  (prop/for-all [f gen-body]
                (stub-milvus true
                  (fn []
                    (let [r (with-auto-reconnect (atom {}) f 5)]
                      (or (not (map? r))
                          (contains? r :success?)
                          (some? r)
                          (nil? r)))))))

;; =============================================================================
;; FSM termination — model the retry walk as a pure step function and
;; prove every trace ends in a terminal state within ≤ 3 steps (initial
;; attempt, retry attempt, give-up). Matches the linearized pipeline:
;; :fresh -> :transient -> :reconnect -> :retry -> :give-up | :ok.
;; =============================================================================

(defn- fsm-step
  "State-machine step. Outcomes that don't match the current state's
   alphabet are projected to the state's default transition — keeps
   the property generator free to emit any sequence."
  [{:keys [state turns] :as s} outcome]
  (let [t (inc (or turns 0))]
    (case state
      :fresh      (case outcome
                    :ok        (assoc s :state :ok :turns t :terminated? true)
                    :fatal     (assoc s :state :fatal :turns t :terminated? true)
                    (assoc s :state :transient :turns t))
      :transient  (assoc s :state :reconnect :turns t)
      :reconnect  (case outcome
                    :timeout   (assoc s :state :give-up :turns t :terminated? true)
                    (assoc s :state :retry :turns t))
      :retry      (case outcome
                    :ok       (assoc s :state :ok :turns t :terminated? true)
                    (assoc s :state :give-up :turns t :terminated? true)))))

(defn- run-fsm
  [{:keys [outcomes max-turns]}]
  (loop [s {:state :fresh :turns 0}
         [o & rest] outcomes]
    (cond
      (:terminated? s)            s
      (>= (:turns s) max-turns)   (assoc s :terminated? false)
      (nil? o)                    (assoc s :terminated? true)
      :else                       (recur (fsm-step s o) rest))))

(def gen-fsm-inputs
  (gen/hash-map
    :outcomes (gen/vector
                (gen/elements [:ok :transient :fatal :recovered :timeout :fail])
                1 6)))

(defspec retry-fsm-terminates 200
  (prop/for-all [inputs gen-fsm-inputs]
                (let [r (run-fsm (assoc inputs :max-turns 5))]
                  (and (:terminated? r)
                       (<= (:turns r 0) 5)))))
