(ns hive-milvus.trifecta-test
  "Consolidated trifecta tests for MilvusMemoryStore pure functions.

   Each deftrifecta generates golden + property + mutation tests from
   a single declaration. Compare with the ~250 lines across golden_test.clj,
   property_test.clj, and mutation_test.clj that test the same functions."
  (:require [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [hive-test.trifecta :refer [deftrifecta deftest-facets]]
            [hive-test.properties :as props]
            [hive-test.generators.memory :as gen-mem]
            [hive-milvus.store :as milvus-store]
            [hive-mcp.protocols.memory :as proto]))

;; =============================================================================
;; 1. tags->str: golden + totality + mutation
;;    Roundtrip (tags->str / str->tags) kept as separate defprop-roundtrip
;;    because it's inherently a two-function pattern.
;; =============================================================================

(deftrifecta tags-serialization
  hive-milvus.store/tags->str
  {:golden-path "test/golden/milvus/trifecta-tags-serialization.edn"
   :cases       {:empty-vec  []
                 :single     ["alpha"]
                 :multi      ["alpha" "beta" "gamma"]
                 :with-colon ["scope:project:hive"]
                 :nil-input  nil}
   :gen         (gen/one-of [gen-mem/gen-tags
                             (gen/return nil)
                             (gen/return [])])
   :mutations   [["always-empty" (fn [_] "[]")]
                 ["always-nil"   (fn [_] nil)]
                 ["drops-first"  (fn [tags]
                                   (if (and (sequential? tags) (seq tags))
                                     (clojure.data.json/write-str (rest tags))
                                     "[]"))]]})

;; Roundtrip: two-function pattern → stays as individual defprop-roundtrip
(props/defprop-roundtrip tags-roundtrip
  milvus-store/tags->str
  milvus-store/str->tags
  gen-mem/gen-tags
  {:num-tests 100})

;; =============================================================================
;; 2. build-filter-expr: golden + idempotent + mutation
;; =============================================================================

;; Note: Cases use include-expired? true to avoid non-deterministic timestamps.
;; Time-dependent cases (include-expired? false) are covered by the original golden_test.clj.
;; build-filter-expr is map→string, NOT idempotent — use :pred instead.
(deftrifecta filter-expr
  hive-milvus.store/build-filter-expr
  {:golden-path  "test/golden/milvus/trifecta-filter-expressions.edn"
   :cases        {:type-only         {:type :decision :include-expired? true}
                  :project-id        {:project-id "hive-mcp" :include-expired? true}
                  :project-ids       {:project-ids ["hive-mcp" "hive-test"]
                                      :include-expired? true}
                  :with-tags         {:tags ["migration" "kg"] :include-expired? true}
                  :exclude-tags      {:exclude-tags ["carto"] :include-expired? true}
                  :empty             {:include-expired? true}
                  :combined-no-expiry {:type :decision
                                       :project-id "hive-mcp"
                                       :tags ["architecture"]
                                       :exclude-tags ["deprecated"]
                                       :include-expired? true}}
   :gen          (gen/let [type gen-mem/gen-memory-type]
                   {:type type :include-expired? true})
   :pred         #(or (nil? %) (and (string? %) (pos? (count %))))
   :num-tests    100
   :mutations    [["always-nil"    (fn [_] nil)]
                  ["always-empty"  (fn [_] "")]
                  ["drops-type"    (fn [opts]
                                     (milvus-store/build-filter-expr
                                       (dissoc opts :type)))]]})

;; =============================================================================
;; 3. staleness-probability: golden + totality/pred + mutation
;;    Monotonic property kept separate (two-call comparison)
;; =============================================================================

(deftrifecta staleness-prob
  hive-milvus.store/staleness-probability
  {:golden-path "test/golden/milvus/trifecta-staleness-probability.edn"
   :cases       {:fresh    {:staleness-alpha 9 :staleness-beta 1}
                 :stale    {:staleness-alpha 1 :staleness-beta 9}
                 :balanced {:staleness-alpha 5 :staleness-beta 5}
                 :defaults {}}
   :gen         (gen/let [alpha (gen/choose 1 1000)
                          beta  (gen/choose 1 1000)]
                  {:staleness-alpha alpha :staleness-beta beta})
   :pred        #(and (number? %) (<= 0.0 % 1.0))
   :num-tests   200
   :mutations   [["always-zero" (fn [_] 0.0)]
                 ["always-one"  (fn [_] 1.0)]
                 ["inverted"    (fn [e]
                                  (let [a (or (:staleness-alpha e) 1)
                                        b (or (:staleness-beta e) 1)]
                                    (/ (double a) (+ a b))))]]})

;; Monotonic: two-call comparison → stays as individual defspec
(clojure.test.check.clojure-test/defspec prop-staleness-monotonic 100
  (clojure.test.check.properties/for-all
    [beta (gen/choose 1 100)]
    (let [p-high (milvus-store/staleness-probability
                   {:staleness-alpha 1 :staleness-beta beta})
          p-low  (milvus-store/staleness-probability
                   {:staleness-alpha beta :staleness-beta 1})]
      (<= p-low p-high))))

;; =============================================================================
;; 4. helpfulness-map: golden + totality + mutation (NEW coverage)
;;    Previously had only golden — trifecta adds property + mutation for free
;; =============================================================================

(deftrifecta helpfulness
  hive-milvus.store/helpfulness-map
  {:golden-path "test/golden/milvus/trifecta-helpfulness-map.edn"
   :cases       {:zero-total  {:helpful-count 0 :unhelpful-count 0}
                 :all-helpful {:helpful-count 10 :unhelpful-count 0}
                 :mixed       {:helpful-count 7 :unhelpful-count 3}
                 :nil-counts  {}}
   :gen         (gen/let [helpful   gen/nat
                          unhelpful gen/nat]
                  {:helpful-count helpful :unhelpful-count unhelpful})
   :pred        #(and (map? %)
                      (contains? % :ratio)
                      (number? (:ratio %))
                      (<= 0.0 (:ratio %) 1.0))
   :num-tests   200
   :mutations   [["always-zero-ratio" (fn [_] {:helpful-count 0 :unhelpful-count 0
                                                :total 0 :ratio 0.0})]
                 ["swapped-counts"    (fn [e]
                                        (milvus-store/helpfulness-map
                                          {:helpful-count (:unhelpful-count e)
                                           :unhelpful-count (:helpful-count e)}))]]})
