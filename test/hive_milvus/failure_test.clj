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
  (:import [io.grpc StatusRuntimeException Status]))

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
