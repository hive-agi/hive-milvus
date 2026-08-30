(ns hive-milvus.store.entries-failure-test
  "The fan-out isolation policy, tested as a function of its inputs.

   The isolation is deliberate and stays: one unreachable collection must not
   sink a query across the others. What was missing is that the caller could
   not tell two empties apart — a document with no rows, and a document whose
   rows were unreachable. Downstream that difference reached a user as
   `no stored chunks found` for a document holding 408 chunks.

   `fan-out` is pure, so nothing here stubs a collaborator or names a real
   collection: the properties hold for any outcomes at all."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [malli.core :as m]
            [hive-milvus.store.entries :as entries]))

;; =============================================================================
;; Generators — shaped by the schema, not by the caller's implementation
;; =============================================================================

(def gen-outcome
  (gen/let [coll  (gen/not-empty gen/string-alphanumeric)
            rows  (gen/vector gen/small-integer 0 5)
            error (gen/one-of [(gen/return nil)
                               (gen/not-empty gen/string-alphanumeric)])]
    (cond-> {:collection coll :rows rows}
      error (assoc :error error))))

(def gen-outcomes (gen/vector gen-outcome 0 8))

;; =============================================================================
;; Properties
;; =============================================================================

(defspec conforms-to-its-declared-shape 200
  (prop/for-all [outcomes gen-outcomes]
                (nil? (m/explain entries/FanOut (entries/fan-out outcomes)))))

(defspec every-row-survives-in-order 200
  ;; Isolation must not cost rows: a failing neighbour cannot drop a healthy
  ;; collection's hits.
  (prop/for-all [outcomes gen-outcomes]
                (= (vec (mapcat :rows outcomes))
                   (:rows (entries/fan-out outcomes)))))

(defspec failures-are-exactly-the-outcomes-carrying-an-error 200
  (prop/for-all [outcomes gen-outcomes]
                (= (mapv :collection (filter :error outcomes))
                   (mapv :collection (:failed (entries/fan-out outcomes))))))

(defspec a-clean-run-reports-no-failures 200
  ;; The property the caller's licence depends on: no error in, no failure out.
  (prop/for-all [outcomes (gen/vector (gen/let [c (gen/not-empty gen/string-alphanumeric)
                                                r (gen/vector gen/small-integer 0 5)]
                                        {:collection c :rows r})
                                      0 8)]
                (empty? (:failed (entries/fan-out outcomes)))))

;; =============================================================================
;; The distinction the bug turned on
;; =============================================================================

(deftest an-empty-result-is-only-trustworthy-without-failures-test
  (testing "genuinely empty — every collection answered and had nothing"
    (let [{:keys [rows failed]} (entries/fan-out [{:collection "a" :rows []}
                                                  {:collection "b" :rows []}])]
      (is (= [] rows))
      (is (= [] failed) "an empty :failed is what licenses 'nothing stored'")))

  (testing "empty because unreachable — indistinguishable by rows alone"
    (let [{:keys [rows failed]} (entries/fan-out [{:collection "a" :rows [] :error "timed out"}
                                                  {:collection "b" :rows []}])]
      (is (= [] rows) "rows look identical to the honest empty above")
      (is (= [{:collection "a" :message "timed out"}] failed)
          "and only :failed separates them")))

  (testing "partial — hits survive alongside a reported failure"
    (let [{:keys [rows failed]} (entries/fan-out [{:collection "a" :rows [1 2]}
                                                  {:collection "b" :rows [] :error "timed out"}])]
      (is (= [1 2] rows))
      (is (= ["b"] (mapv :collection failed))))))
