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
