(ns hive-milvus.relocate.plan-test
  "Trifecta for the drain's pure decisions.

   The property + mutation facets are SYNTHESIZED from the malli schemas by
   hive-schemas.test. Hand-written below only what a schema cannot state: the
   precedence between the four verdicts, and that an id which cannot be safely
   quoted is rejected loudly rather than dropped from the filter."
  (:require [clojure.test :refer [deftest testing is]]
            [hive-schemas.test :as hst]
            [hive-test.mutation :as mut]
            [hive-milvus.relocate.plan :as plan]))

;; ============================================================================
;; Schema-synthesized
;; ============================================================================

;; THE THEOREM: the loop continues only while there is work it may still do.
(hst/deftrifecta-from-schema verdict-is-total-over-every-round
  hive-milvus.relocate.plan/verdict
  {:in  plan/Round
   :out plan/Verdict
   :rel (fn [in out]
          (and
           ;; :continue implies there was a page AND it was classified
           (or (not= out :continue)
               (and (pos? (:page-count in))
                    (pos? (:classified in))
                    (<= (:excluded-count in) (:max-excluded in))
                    (not (:stopping? in))))
           ;; :completed implies nothing came back
           (or (not= out :completed)
               (zero? (:page-count in)))))
   :mutation false
   :num-tests 300})

(mut/deftest-mutations verdict-mutants-are-caught
  hive-milvus.relocate.plan/verdict
  [["calls an over-full exclusion set complete — the drain would claim success"
    (fn [{:keys [page-count stopping?]}]
      (cond stopping? :stopped (zero? page-count) :completed :else :continue))]
   ["never stalls — an unmovable tail loops forever"
    (fn [{:keys [page-count stopping?]}]
      (cond stopping? :stopped (zero? page-count) :completed :else :continue))]
   ["ignores the stop request"
    (fn [{:keys [page-count]}]
      (if (zero? page-count) :completed :continue))]]
  (fn []
    ;; Each conjunct is pinned by its own witness: a mutant that drops any one
    ;; of them must show up as a different verdict on at least one of these.
    (let [over    {:page-count 10 :classified 10 :excluded-count 600
                   :max-excluded 500 :stopping? false}
          halting {:page-count 10 :classified 10 :excluded-count 0
                   :max-excluded 500 :stopping? true}
          working {:page-count 10 :classified 10 :excluded-count 0
                   :max-excluded 500 :stopping? false}]
      (is (= :stalled  (plan/verdict over)))
      (is (= :stopped  (plan/verdict halting)))
      (is (= :continue (plan/verdict working))))))

;; ============================================================================
;; Hand-written — precedence and the filter's refusal to lie
;; ============================================================================

(deftest a-stop-request-outranks-everything
  (testing "a stop is honoured even when the source still has rows"
    (is (= :stopped (plan/verdict {:page-count 100 :classified 100
                                   :excluded-count 0 :max-excluded 500
                                   :stopping? true})))))

(deftest an-over-full-exclusion-set-is-a-stall-not-a-completion
  (testing "once we can no longer exclude reliably, we stop and say so"
    ;; The page is empty ONLY because the loop refused to fetch. Calling that
    ;; :completed is exactly the false success this drain exists to kill.
    (is (= :stalled (plan/verdict {:page-count 0 :classified 0
                                   :excluded-count 501 :max-excluded 500
                                   :stopping? false})))))

(deftest an-empty-page-completes
  (testing "nothing left that is not already excluded"
    (is (= :completed (plan/verdict {:page-count 0 :classified 0
                                     :excluded-count 3 :max-excluded 500
                                     :stopping? false})))))

(deftest a-page-nobody-could-classify-stalls
  (testing "rows came back but none were processed — do not spin"
    (is (= :stalled (plan/verdict {:page-count 10 :classified 0
                                   :excluded-count 0 :max-excluded 500
                                   :stopping? false})))))

(deftest empty-exclusion-set-selects-everything
  (is (= "id != \"\"" (plan/exclusion-filter []))))

(deftest every-excluded-id-appears-in-the-filter
  (let [f (plan/exclusion-filter ["20260712003305-15616872" "20260711225051-0620645a"])]
    (is (re-find #"not in \[" f))
    (is (re-find #"\"20260712003305-15616872\"" f))
    (is (re-find #"\"20260711225051-0620645a\"" f))))

(deftest an-unquotable-id-throws-rather-than-vanishing
  (testing "a dropped id would be handed back forever — refuse instead"
    (is (thrown? clojure.lang.ExceptionInfo
                 (plan/exclusion-filter ["ok-id" "evil\" or 1==1"])))))
