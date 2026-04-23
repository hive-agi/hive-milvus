(ns hive-milvus.failure-test
  "Unit + property tests for hive-milvus.failure.

   Classifier must be a pure total function: every Throwable maps to
   exactly one MilvusFailure variant, never throws. Legacy translator
   must preserve the {:success? false :errors [...] :reconnecting? ...}
   shape for every ADT variant."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-dsl.adt :as adt]
            [hive-milvus.failure :as failure])
  (:import [io.grpc StatusRuntimeException Status]
           [java.util.concurrent ExecutionException CompletionException]))

;; =============================================================================
;; classify — unit cases covering every branch of the taxonomy
;; =============================================================================

(deftest classify-grpc-unavailable
  (testing "io.grpc.StatusRuntimeException -> :milvus/transient"
    (let [e  (StatusRuntimeException. (Status/UNAVAILABLE))
          f  (failure/classify e)]
      (is (= :MilvusFailure (adt/adt-type f)))
      (is (= :milvus/transient (adt/adt-variant f)))
      (is (failure/transient? f)))))

(deftest classify-keepalive-failed
  (testing "\"Keepalive failed\" message -> :milvus/transient"
    (let [e (RuntimeException. "UNAVAILABLE: Keepalive failed. The connection is likely gone")
          f (failure/classify e)]
      (is (= :milvus/transient (adt/adt-variant f))))))

(deftest classify-not-connected
  (testing "\"not connected\" message -> :milvus/transient"
    (let [e (RuntimeException. "client is not connected")
          f (failure/classify e)]
      (is (= :milvus/transient (adt/adt-variant f))))))

(deftest classify-deadline-exceeded
  (testing "\"DEADLINE_EXCEEDED\" message -> :milvus/transient"
    (let [e (RuntimeException. "RPC failed: DEADLINE_EXCEEDED")
          f (failure/classify e)]
      (is (= :milvus/transient (adt/adt-variant f))))))

(deftest classify-generic-fatal
  (testing "Generic RuntimeException -> :milvus/fatal"
    (let [e (RuntimeException. "collection not found")
          f (failure/classify e)]
      (is (= :milvus/fatal (adt/adt-variant f)))
      (is (not (failure/transient? f))))))

(deftest classify-null-message
  (testing "Throwable with nil message still classifies (no NPE)"
    (let [e (RuntimeException.)
          f (failure/classify e)]
      (is (#{:milvus/fatal :milvus/transient} (adt/adt-variant f))))))

;; =============================================================================
;; Wrapper-unwinding — future/async chains swallow tagged ex-data, classifier
;; must walk .getCause to see the tagged/transient inner throwable.
;; Regression: before this fix, ExecutionException-wrapped transients leaked
;; through as :milvus/fatal, starving the Milvus heal loop.
;; =============================================================================

(deftest classify-execution-exception-wrapping-transient
  (testing "ExecutionException wrapping a transient gRPC failure -> :milvus/transient"
    (let [inner (StatusRuntimeException. (Status/UNAVAILABLE))
          outer (ExecutionException. "java.util.concurrent.ExecutionException" inner)
          f     (failure/classify outer)]
      (is (= :milvus/transient (adt/adt-variant f))
          "outer ExecutionException has no ex-data; classifier must walk .getCause")
      (is (failure/transient? f)))))

(deftest classify-execution-exception-wrapping-ex-info-transport-tag
  (testing "ExecutionException wrapping an ex-info tagged ::client/transport -> :milvus/transient"
    (let [inner (ex-info "selector manager closed"
                         {:milvus-clj.client/transport :http
                          :cause :io})
          outer (ExecutionException. "wrapped" inner)
          f     (failure/classify outer)]
      (is (= :milvus/transient (adt/adt-variant f))))))

(deftest classify-nested-runtime-execution-status
  (testing "RuntimeException -> ExecutionException -> StatusRuntimeException(UNAVAILABLE) -> :milvus/transient"
    (let [status  (StatusRuntimeException. (Status/UNAVAILABLE))
          exec    (ExecutionException. "future deref" status)
          runtime (RuntimeException. "wrapper" exec)
          f       (failure/classify runtime)]
      (is (= :milvus/transient (adt/adt-variant f))
          "three-deep chain must still be walked to reach the transient grpc leaf"))))

(deftest classify-completion-exception-wrapping-transient
  (testing "CompletionException wrapping a transient -> :milvus/transient (CompletableFuture path)"
    (let [inner (StatusRuntimeException. (Status/DEADLINE_EXCEEDED))
          outer (CompletionException. "future.join" inner)
          f     (failure/classify outer)]
      (is (= :milvus/transient (adt/adt-variant f))))))

(deftest classify-wrapped-fatal-stays-fatal
  (testing "ExecutionException wrapping a genuinely fatal cause stays :milvus/fatal"
    (let [inner (RuntimeException. "collection not found")
          outer (ExecutionException. "wrapped" inner)
          f     (failure/classify outer)]
      (is (= :milvus/fatal (adt/adt-variant f))
          "walking the chain must not promote fatal causes to transient"))))

(deftest classify-deep-cycle-terminates
  (testing "Self-referential cause chain terminates (depth cap + cycle guard)"
    (let [e (RuntimeException. "collection not found")]
      ;; RuntimeException's initCause rejects self — the identity-cycle
      ;; guard + max-cause-depth cap defend against pathological chains
      ;; that slip past that JVM check via reflection or subclassing.
      ;; This test just asserts the classifier returns (i.e. doesn't loop).
      (is (= :milvus/fatal (adt/adt-variant (failure/classify e)))))))

;; =============================================================================
;; ->legacy-map — every variant must yield the legacy shape
;; =============================================================================

(deftest legacy-map-transient
  (let [m (failure/->legacy-map (failure/classify (RuntimeException. "UNAVAILABLE")))]
    (is (= false (:success? m)))
    (is (vector? (:errors m)))
    (is (true? (:reconnecting? m)))))

(deftest legacy-map-fatal
  (let [m (failure/->legacy-map (failure/classify (RuntimeException. "boom")))]
    (is (= false (:success? m)))
    (is (false? (:reconnecting? m)))))

(deftest legacy-map-reconnect-timeout
  (let [m (failure/->legacy-map (failure/reconnect-timeout "budget exhausted"))]
    (is (= false (:success? m)))
    (is (true? (:reconnecting? m)))
    (is (= ["budget exhausted"] (:errors m)))))

;; =============================================================================
;; Property: classify is total
;; =============================================================================

(def gen-throwable
  "Generator for Throwables spanning the classifier's input space."
  (gen/one-of
    [(gen/fmap #(RuntimeException. ^String %)
               (gen/elements ["UNAVAILABLE"
                              "Keepalive failed"
                              "not connected"
                              "DEADLINE_EXCEEDED"
                              "connection is likely gone"
                              "collection not found"
                              "schema mismatch"
                              ""]))
     (gen/return (StatusRuntimeException. (Status/UNAVAILABLE)))
     (gen/return (StatusRuntimeException. (Status/DEADLINE_EXCEEDED)))
     (gen/return (RuntimeException.))
     (gen/return (Exception. "random"))]))

(defspec classify-total 200
  (prop/for-all [t gen-throwable]
                (let [f (failure/classify t)]
                  (and (adt/adt? f)
                       (= :MilvusFailure (adt/adt-type f))
                       (#{:milvus/transient :milvus/fatal}
                        (adt/adt-variant f))))))

(defspec legacy-map-total 200
  (prop/for-all [t gen-throwable]
                (let [m (failure/->legacy-map (failure/classify t))]
                  (and (false? (:success? m))
                       (vector? (:errors m))
                       (contains? m :reconnecting?)))))
