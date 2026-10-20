(ns hive-milvus.queue-write-failure-contract-test
  "Trifecta (golden + property + mutation) pinning `hive-milvus.queue/write-failure?`
   to the contract's `hive-spi.memory.contract/failure?`.

   The property generates AddResult answers (an id, a queue acknowledgement or
   a refusal), plus off-contract values, and checks both predicates agree on
   every one. The mutations are plausible drifts of a hand-rolled mirror; each
   must disagree with the golden."
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-milvus.queue :as q]
            [hive-spi.memory.contract :as contract]
            [hive-test.trifecta :refer [deftrifecta]]
            [malli.core :as m]))

;; =============================================================================
;; Generators — AddResult answers
;; =============================================================================

(def ^:private gen-id
  "contract/Id: a non-empty string."
  (gen/fmap #(str "id-" %) gen/string-alphanumeric))

(def ^:private gen-error
  (gen/one-of [gen/keyword
               (gen/fmap str gen/string-alphanumeric)
               (gen/return {:cause :timeout})]))

(def ^:private gen-queued
  "contract/Queued: :queued? true and not a failure."
  (gen/let [depth gen/nat
            ok?   (gen/elements [true nil])]
    (cond-> {:queued? true :depth depth}
      ok? (assoc :success? true))))

(def ^:private gen-failure
  "contract/Failure: :success? false and/or a non-nil :error."
  (gen/one-of
   [(gen/fmap (fn [e] {:success? false :errors [e]}) gen-error)
    (gen/fmap (fn [e] {:error e}) gen-error)
    (gen/fmap (fn [e] {:success? false :error e}) gen-error)
    ;; A degraded write: queued but refused — still a failure.
    (gen/fmap (fn [e] {:success? false :queued? true :error e}) gen-error)]))

(def ^:private gen-add-result
  (gen/one-of [gen-id gen-queued gen-failure]))

(def ^:private gen-off-contract
  "Values outside AddResult; the predicates must still agree on them."
  (gen/one-of [(gen/return nil) gen/boolean gen/small-integer
               (gen/return {}) (gen/return {:success? nil :error nil})
               (gen/return {:success? true}) (gen/return [:success? false])]))

(def ^:private gen-answer
  (gen/frequency [[4 gen-add-result] [1 gen-off-contract]]))

;; =============================================================================
;; Both verdicts side by side
;; =============================================================================

(defn- verdicts
  [x]
  {:queue    (q/write-failure? x)
   :contract (contract/failure? x)})

(defn- agree? [{:keys [queue contract]}]
  (and (boolean? queue) (= queue contract)))

;; =============================================================================
;; Trifecta on write-failure? itself
;; =============================================================================

(deftrifecta write-failure-trifecta
  hive-milvus.queue/write-failure?
  {:golden-path "test/golden/milvus/trifecta-queue-write-failure.edn"
   :cases       {:id               "id-1"
                 :queued           {:queued? true :depth 3}
                 :queued-success   {:success? true :queued? true :depth 1}
                 :success-false    {:success? false :errors ["boom"]}
                 :error-only       {:error :circuit-open}
                 :error-and-false  {:success? false :error "x"}
                 :queued-refused   {:success? false :queued? true :error :queue-full}
                 :entry            {:id "x" :content "c"}
                 :success-nil      {:success? nil :error nil}
                 :nil              nil
                 :true             true
                 :vector           [:success? false]}
   :gen         gen-answer
   :pred        boolean?
   :num-tests   300
   :mutations   [["always-false"      (fn [_] false)]
                 ["ignores-error"     (fn [x] (and (map? x) (false? (:success? x))))]
                 ["ignores-success"   (fn [x] (and (map? x) (some? (:error x))))]
                 ["falsy-success"     (fn [x] (and (map? x)
                                                   (or (not (:success? x))
                                                       (some? (:error x)))))]
                 ["queued-is-failure" (fn [x] (and (map? x)
                                                   (or (false? (:success? x))
                                                       (some? (:error x))
                                                       (true? (:queued? x)))))]]})

;; =============================================================================
;; Property: both predicates agree on generated AddResult answers
;; =============================================================================

(defspec add-result-generator-is-on-contract 200
  (prop/for-all [r gen-add-result]
    (m/validate contract/AddResult r)))

(defspec write-failure-agrees-with-contract-on-add-results 500
  (prop/for-all [r gen-add-result]
    (agree? (verdicts r))))

(defspec write-failure-agrees-with-contract-off-contract 200
  (prop/for-all [r gen-off-contract]
    (agree? (verdicts r))))

(deftest write-failure-classifies-like-outcome
  (doseq [r [(gen/generate gen-failure) (gen/generate gen-queued) "id-1"]]
    (is (= (q/write-failure? r)
           (= :failed (contract/outcome :add-entry! r))))))
