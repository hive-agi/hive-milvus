(ns hive-milvus.ensure-live-test
  "Unit tests for the preemptive liveness gate, `resilience.retry/ensure-live!`.

   No live Milvus required: the gate's only two collaborators are
   `probe/alive?` (a cached boolean) and `reconnect/kick!` (an effect), and
   both are redefined here.

   The gate's contract is three clauses:
     alive                      -> true,  no kick
     dead, loop dormant         -> false, exactly one kick
     dead, loop already running -> false, no kick
   plus totality: a probe that throws counts as dead rather than escaping."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-milvus.resilience.probe :as probe]
            [hive-milvus.resilience.reconnect :as reconnect]
            [hive-milvus.resilience.retry :as retry]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(defn- reset-state! [f]
  (reset! reconnect/reconnect-state {:running? false :future nil :last-attempt nil})
  (f))

(use-fixtures :each reset-state!)

(defn- with-probe
  "Run `f` with `probe/alive?` answering `alive-fn` and `reconnect/kick!`
   counting into an atom. Returns [result kick-count]."
  [alive-fn f]
  (let [kicks (atom 0)]
    (with-redefs [probe/alive?    alive-fn
                  reconnect/kick! (fn [_] (swap! kicks inc) nil)]
      [(f) @kicks])))

(deftest alive-probe-returns-true-and-does-not-kick
  (testing "a live client needs no healing"
    (let [[result kicks] (with-probe (constantly true)
                                     #(retry/ensure-live! (atom {})))]
      (is (true? result))
      (is (zero? kicks) "kick! must not run while the client is alive"))))

(deftest dead-probe-kicks-the-reconnect-loop
  (testing "a dead client starts healing before the caller's RPC fails"
    (let [[result kicks] (with-probe (constantly false)
                                     #(retry/ensure-live! (atom {})))]
      (is (false? result))
      (is (= 1 kicks) "a dormant loop must be kicked exactly once"))))

(deftest running-loop-is-not-double-kicked
  (testing "kick is suppressed while the loop is already running"
    (swap! reconnect/reconnect-state assoc :running? true)
    (let [[result kicks] (with-probe (constantly false)
                                     #(retry/ensure-live! (atom {})))]
      (is (false? result))
      (is (zero? kicks) "must not start a second reconnect loop"))))

(deftest a-throwing-probe-counts-as-dead
  (testing "ensure-live! is total: it never propagates the probe's throwable"
    (let [[result kicks] (with-probe (fn [] (throw (RuntimeException. "boom")))
                                     #(retry/ensure-live! (atom {})))]
      (is (false? result)
          "a probe that cannot vouch for the client reads as dead")
      (is (= 1 kicks) "and healing starts, exactly as for any dead reading"))))

(deftest gate-does-not-block-on-recovery
  (testing "ensure-live! returns immediately, it does not await the loop"
    (let [start (System/nanoTime)
          [result _] (with-probe (constantly false)
                                 #(retry/ensure-live! (atom {})))
          elapsed-ms (/ (- (System/nanoTime) start) 1e6)]
      (is (false? result))
      (is (< elapsed-ms 500)
          "the reactive path owns the retry budget, the gate must not wait"))))
