(ns hive-milvus.ensure-live-test
  "Unit tests for the preemptive liveness check (`ensure-live!`) added to
   the `resilient` macro in hive-milvus.store.

   These tests exercise the cache + kick-reconnect path in isolation —
   no live Milvus required. They use with-redefs to stub milvus/connected?,
   milvus/disconnect!, and start-reconnect-loop!."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-milvus.store :as store]
            [milvus-clj.api :as milvus]))

;; Grab private vars by name — tests live in a sibling ns.
(def health-cache            @#'store/health-cache)
(def health-cache-ttl-ms     @#'store/health-cache-ttl-ms)
(def ensure-live!            @#'store/ensure-live!)
(def kick-reconnect-now!     @#'store/kick-reconnect-now!)
(def invalidate-health-cache! @#'store/invalidate-health-cache!)
(def reconnect-state         @#'store/reconnect-state)

(defn- reset-state! [f]
  (reset! health-cache {:ts 0 :alive? false})
  (reset! reconnect-state {:running? false :future nil :last-attempt nil})
  (f))

(use-fixtures :each reset-state!)

(deftest cache-hit-skips-check
  (testing "fresh positive cache → no call to milvus/connected?"
    (let [calls (atom 0)]
      (reset! health-cache {:ts (System/currentTimeMillis) :alive? true})
      (with-redefs [milvus/connected? (fn [] (swap! calls inc) true)
                    milvus/disconnect! (fn [] nil)]
        (is (true? (ensure-live! (atom {}))))
        (is (zero? @calls) "connected? must not be called on cache hit")))))

(deftest cache-miss-triggers-recheck
  (testing "stale cache → re-verifies via milvus/connected?"
    (let [calls (atom 0)]
      (reset! health-cache {:ts 0 :alive? true}) ; stale — ts way in past
      (with-redefs [milvus/connected? (fn [] (swap! calls inc) true)
                    milvus/disconnect! (fn [] nil)]
        (is (true? (ensure-live! (atom {}))))
        (is (= 1 @calls))
        (is (true? (:alive? @health-cache)))))))

(deftest dead-client-kicks-reconnect
  (testing "milvus/connected? = false → kick-reconnect-now! runs"
    (let [disconnect-calls (atom 0)
          loop-calls (atom 0)]
      (with-redefs [milvus/connected?          (fn [] false)
                    milvus/disconnect!         (fn [] (swap! disconnect-calls inc))
                    store/start-reconnect-loop! (fn [& _] (swap! loop-calls inc))]
        (is (false? (ensure-live! (atom {}))))
        (is (= 1 @disconnect-calls) "dead client must be dropped")
        (is (= 1 @loop-calls)       "reconnect loop must be started")
        (is (false? (:alive? @health-cache)))))))

(deftest running-loop-not-double-kicked
  (testing "reconnect loop already running → kick is a no-op"
    (let [loop-calls (atom 0)]
      (swap! reconnect-state assoc :running? true)
      (with-redefs [milvus/connected?          (fn [] false)
                    milvus/disconnect!         (fn [] nil)
                    store/start-reconnect-loop! (fn [& _] (swap! loop-calls inc))]
        (ensure-live! (atom {}))
        (is (zero? @loop-calls) "must not start a second reconnect loop")))))

(deftest cache-ttl-window
  (testing "positive reading within TTL is trusted; outside TTL re-checks"
    (let [calls (atom 0)
          now   (System/currentTimeMillis)]
      (reset! health-cache {:ts (- now 1000) :alive? true}) ; 1s ago — fresh
      (with-redefs [milvus/connected? (fn [] (swap! calls inc) true)
                    milvus/disconnect! (fn [] nil)]
        (ensure-live! (atom {}))
        (is (zero? @calls)))
      (reset! health-cache {:ts (- now health-cache-ttl-ms 500) :alive? true}) ; stale
      (with-redefs [milvus/connected? (fn [] (swap! calls inc) true)
                    milvus/disconnect! (fn [] nil)]
        (ensure-live! (atom {}))
        (is (= 1 @calls))))))

(deftest invalidate-forces-recheck
  (testing "invalidate-health-cache! resets alive? so next call re-verifies"
    (reset! health-cache {:ts (System/currentTimeMillis) :alive? true})
    (invalidate-health-cache!)
    (is (false? (:alive? @health-cache)))
    (is (zero?  (:ts @health-cache)))))

(deftest connected-check-exception-is-safe
  (testing "milvus/connected? throwing → treated as dead, kicks reconnect"
    (let [loop-calls (atom 0)]
      (with-redefs [milvus/connected?          (fn [] (throw (RuntimeException. "boom")))
                    milvus/disconnect!         (fn [] nil)
                    store/start-reconnect-loop! (fn [& _] (swap! loop-calls inc))]
        (is (false? (ensure-live! (atom {}))))
        (is (= 1 @loop-calls))))))
