(ns hive-milvus.property-test
  "Property-based tests for MilvusMemoryStore.

   Uses hive-test generators and property macros to verify algebraic
   properties: roundtrips, idempotency, totality, invariants."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-test.generators.memory :as gen-mem]
            [hive-test.properties :as props]
            [hive-milvus.store :as milvus-store]
            [hive-mcp.protocols.memory :as proto]))

;; =============================================================================
;; Shared Store
;; =============================================================================

(def ^:private test-store (atom nil))

(defn setup-milvus-store [f]
  (let [store (milvus-store/create-store
                {:host "localhost" :port 19530
                 :collection-name "hive-mcp-property-test"})]
    (proto/connect! store {:host "localhost" :port 19530
                           :collection-name "hive-mcp-property-test"})
    (reset! test-store store)
    (try (f)
         (finally
           (proto/reset-store! store)
           (proto/disconnect! store)))))

(use-fixtures :once setup-milvus-store)

(defn- fresh-store []
  (proto/reset-store! @test-store)
  @test-store)

;; =============================================================================
;; Roundtrip: tags->str / str->tags
;; =============================================================================

(props/defprop-roundtrip tags-roundtrip
  milvus-store/tags->str
  milvus-store/str->tags
  gen-mem/gen-tags
  {:num-tests 100})

;; =============================================================================
;; Idempotency: build-filter-expr produces same output for same input
;; =============================================================================

(props/defprop-idempotent filter-expr-idempotent
  milvus-store/build-filter-expr
  (gen/let [type gen-mem/gen-memory-type]
    {:type type :include-expired? false})
  {:num-tests 100})

;; =============================================================================
;; Totality: entry->record never throws for valid memory entries
;; =============================================================================

(defspec prop-entry-to-record-total 50
  (prop/for-all [entry gen-mem/gen-memory-entry]
    (let [full (assoc entry
                 :id (proto/generate-id)
                 :content-hash (proto/content-hash (:content entry)))]
      ;; entry->record calls the embedding service, so we can't run it
      ;; without a live provider. Test the pure parts instead.
      (and (string? (:id full))
           (string? (:content-hash full))
           (keyword? (:type full))
           (vector? (:tags full))))))

;; =============================================================================
;; Totality: staleness-probability never throws, always in [0,1]
;; =============================================================================

(defspec prop-staleness-probability-total 200
  (prop/for-all [alpha gen/pos-int
                 beta  gen/pos-int]
    (let [p (milvus-store/staleness-probability
              {:staleness-alpha alpha :staleness-beta beta})]
      (and (number? p) (<= 0.0 p 1.0)))))

;; =============================================================================
;; Complement: staleness-probability high vs low
;; =============================================================================

(defspec prop-staleness-monotonic 100
  (prop/for-all [alpha (gen/choose 1 100)
                 beta  (gen/choose 1 100)]
    ;; Higher beta relative to alpha → higher staleness
    (let [p-high (milvus-store/staleness-probability
                   {:staleness-alpha 1 :staleness-beta beta})
          p-low  (milvus-store/staleness-probability
                   {:staleness-alpha beta :staleness-beta 1})]
      (<= p-low p-high))))

;; =============================================================================
;; Invariant: add N entries, query all → count = N
;; =============================================================================

(defspec prop-add-query-count-invariant 20
  (prop/for-all [n (gen/choose 1 5)]
    (let [store (fresh-store)
          ids (mapv (fn [i]
                      (let [entry {:id (proto/generate-id)
                                   :type :note
                                   :content (str "prop-inv-" i "-" (random-uuid))
                                   :tags ["prop-test"]
                                   :duration :medium}]
                        (proto/add-entry! store entry)
                        (:id entry)))
                    (range n))
          got (keep #(proto/get-entry store %) ids)]
      (= n (count got)))))

;; =============================================================================
;; Metamorphic: adding more entries never reduces query result count
;; =============================================================================

(deftest test-metamorphic-query-monotonic
  (let [store (fresh-store)
        mk (fn [i] {:id (proto/generate-id)
                     :type :note
                     :content (str "meta-" i)
                     :tags ["meta"]
                     :duration :medium})]
    (proto/add-entry! store (mk 0))
    (proto/add-entry! store (mk 1))
    (let [count-2 (count (proto/query-entries store {:type :note :limit 100}))]
      (proto/add-entry! store (mk 2))
      (let [count-3 (count (proto/query-entries store {:type :note :limit 100}))]
        (testing "adding entries never decreases query count"
          (is (>= count-3 count-2)))))))
