(ns hive-milvus.test-isolation
  "Shared test fixture: return the process-wide resilience singletons to
   their at-rest state around every test.

   The circuit breaker (`hive-milvus.circuit`) and the reconnect loop
   (`hive-milvus.resilience.reconnect/reconnect-state`) are JVM-global.
   Integration tests that cannot reach localhost:19530 record failures
   until the breaker trips :open (15 s cooldown) and kick the background
   heal loop. Without a reset, any unit namespace that kaocha schedules
   after them inherits the open breaker (every wrapped write answers
   `:circuit-open` without running) and a heal loop that may `dial!` a real
   transport over the fake client slot. Results then depend on test order.

   Use it in every namespace that drives a MilvusMemoryStore or the
   `resilient` wrapper:

     (use-fixtures :each iso/isolate-resilience)

   or compose it with a namespace's own :each fixture via
   `clojure.test/join-fixtures`."
  (:require [hive-milvus.circuit :as circuit]
            [hive-milvus.resilience.probe :as probe]
            [hive-milvus.resilience.reconnect :as reconnect]))

(defn reset-resilience!
  "Close the breaker, stop and cancel any heal loop, and drop the cached
   liveness verdict. Does not touch the milvus-clj client slot: fixtures
   that install a fake client own that."
  []
  (circuit/force-reset!)
  (reconnect/stop!)
  (some-> (:future @reconnect/reconnect-state) future-cancel)
  (reset! reconnect/reconnect-state
          {:running? false :future nil :last-attempt nil :attempt 0})
  (try (probe/invalidate!) (catch Throwable _ nil))
  nil)

(defn isolate-resilience
  "clojure.test fixture: run `f` between two `reset-resilience!` calls, so
   a test neither inherits nor leaks breaker / reconnect-loop state."
  [f]
  (reset-resilience!)
  (try (f) (finally (reset-resilience!))))

(defn- reachable?
  "True when host:port accepts a TCP connection within 500 ms. A refused or
   timed-out connect is the answer `false`, not an error."
  [^String host port]
  (try
    (with-open [s (java.net.Socket.)]
      (.connect s (java.net.InetSocketAddress. host (int port)) 500)
      true)
    (catch java.io.IOException _unreachable false)))

(defn isolate-live
  "clojure.test :each fixture for the integration namespaces that need a
   Milvus at localhost:19530. Resets the resilience singletons like
   `isolate-resilience`. When nothing listens there, the breaker is then
   held :open with an unbounded cooldown for the test, so every wrapped
   call fails fast with `:circuit-open`. Without that, whether a test met
   a closed, open or half-open breaker depended on how many failures
   earlier tests had recorded and on the 15 s cooldown clock, so the
   failing set drifted between runs. With a server present the breaker
   is left closed and the tests run for real."
  [f]
  (reset-resilience!)
  (when-not (reachable? "localhost" 19530)
    (circuit/configure! {:threshold 1 :cooldown-ms Long/MAX_VALUE})
    (circuit/record-failure!))
  (try (f) (finally (reset-resilience!))))
