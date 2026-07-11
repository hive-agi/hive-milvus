(ns hive-milvus.resilience.scheduler-test
  "Unit tests for the proactive idle-liveness scheduler.

   Isolated from a real Milvus: every test redefs `retry/ensure-live!` (the
   delegated health policy) and `probe/invalidate!` so nothing touches the
   singleton client. We assert the scheduler's OWN responsibilities — timer
   lifecycle + delegation + never-throw — not the policy it delegates to
   (that is covered by the retry/reconnect tests)."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-milvus.resilience.scheduler :as sched]
            [hive-milvus.resilience.probe :as probe]
            [hive-milvus.resilience.retry :as retry]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; The scheduler-state is a process-global defonce — make sure no test leaks a
;; running timer into the next one.
(use-fixtures :each
  (fn [t]
    (sched/stop!)
    (t)
    (sched/stop!)))

(deftest health-tick-delegates-and-never-throws
  (testing "health-tick! returns exactly what ensure-live! returns"
    (with-redefs [probe/invalidate!  (constantly nil)
                  retry/ensure-live! (constantly true)]
      (is (true? (sched/health-tick! (atom {})))))
    (with-redefs [probe/invalidate!  (constantly nil)
                  retry/ensure-live! (constantly false)]
      (is (false? (sched/health-tick! (atom {}))))))
  (testing "a throwing policy is rescued to false — a tick must never kill its own scheduler"
    (with-redefs [probe/invalidate!  (constantly nil)
                  retry/ensure-live! (fn [_] (throw (ex-info "boom" {})))]
      (is (false? (sched/health-tick! (atom {})))))))

(deftest start-stop-lifecycle-is-idempotent
  (testing "interval <= 0 disables the scheduler"
    (is (false? (:started (sched/start! (atom {}) 0))))
    (is (false? (:running? (sched/status)))))
  (testing "start! begins, is idempotent, stop! ends, stop! is idempotent"
    (with-redefs [probe/invalidate!  (constantly nil)
                  retry/ensure-live! (constantly true)]
      ;; 3600s cadence: the timer never fires during the test, so the
      ;; lifecycle assertions stay deterministic.
      (is (true? (:started (sched/start! (atom {}) 3600))))
      (is (true? (:running? (sched/status))))
      (is (= "already-running" (:reason (sched/start! (atom {}) 3600))))
      (is (true? (:stopped (sched/stop!))))
      (is (false? (:running? (sched/status))))
      (is (= "not-running" (:reason (sched/stop!)))))))

(deftest timer-fires-and-invokes-delegated-policy
  (testing "the timer actually calls ensure-live! on its cadence"
    (let [calls (atom 0)]
      (with-redefs [probe/invalidate!  (constantly nil)
                    retry/ensure-live! (fn [_] (swap! calls inc) true)]
        (sched/start! (atom {}) 1)   ; 1s cadence (also the initial delay)
        (Thread/sleep 1300)          ; ~one interval — expect >= 1 tick
        (sched/stop!)
        (is (pos? @calls) "ensure-live! was invoked at least once by the timer")
        (is (pos? (:tick-count (sched/status))))))))
