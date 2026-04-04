(ns hive-milvus.golden-test
  "Golden/characterization tests for MilvusMemoryStore.

   Snapshots structural shapes and conversion outputs to detect
   unintended behavioral changes during refactoring."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-test.golden :as golden]
            [hive-milvus.store :as milvus-store]
            [hive-mcp.protocols.memory :as proto]))

;; =============================================================================
;; Golden: tags serialization shape
;; =============================================================================

(golden/deftest-golden tags-serialization-shape
  "test/golden/milvus/tags-serialization.edn"
  {:empty-vec   (milvus-store/tags->str [])
   :single      (milvus-store/tags->str ["alpha"])
   :multi       (milvus-store/tags->str ["alpha" "beta" "gamma"])
   :with-colon  (milvus-store/tags->str ["scope:project:hive"])
   :nil-input   (milvus-store/tags->str nil)})

;; =============================================================================
;; Golden: tags deserialization shape
;; =============================================================================

(golden/deftest-golden tags-deserialization-shape
  "test/golden/milvus/tags-deserialization.edn"
  {:from-json     (milvus-store/str->tags "[\"alpha\",\"beta\"]")
   :empty-array   (milvus-store/str->tags "[]")
   :nil-input     (milvus-store/str->tags nil)
   :blank-input   (milvus-store/str->tags "")})

;; =============================================================================
;; Golden: record->entry conversion shape
;; =============================================================================

(golden/deftest-golden record-to-entry-shape
  "test/golden/milvus/record-to-entry.edn"
  (let [row {:id "20260404-test"
             :type "decision"
             :content "test content"
             :document "test content"
             :tags "[\"tag-a\",\"tag-b\"]"
             :content_hash "abc123"
             :created "2026-04-04T00:00:00Z"
             :updated "2026-04-04T01:00:00Z"
             :duration "long"
             :expires ""
             :access_count 5
             :helpful_count 3
             :unhelpful_count 1
             :project_id "hive-mcp"
             :distance 0.42}]
    (milvus-store/record->entry row)))

;; =============================================================================
;; Golden: record->entry with minimal/nil fields
;; =============================================================================

(golden/deftest-golden record-to-entry-minimal
  "test/golden/milvus/record-to-entry-minimal.edn"
  (milvus-store/record->entry
    {:id "minimal-test"
     :type nil
     :content nil
     :document nil
     :tags nil
     :content_hash nil
     :created nil
     :updated nil
     :duration nil
     :expires nil
     :access_count nil
     :helpful_count nil
     :unhelpful_count nil
     :project_id nil}))

;; =============================================================================
;; Golden: filter expression shapes
;; =============================================================================

(golden/deftest-golden filter-expr-shapes
  "test/golden/milvus/filter-expressions.edn"
  {:type-only     (milvus-store/build-filter-expr
                    {:type :decision :include-expired? true})
   :project-id    (milvus-store/build-filter-expr
                    {:project-id "hive-mcp" :include-expired? true})
   :project-ids   (milvus-store/build-filter-expr
                    {:project-ids ["hive-mcp" "hive-test"]
                     :include-expired? true})
   :with-tags     (milvus-store/build-filter-expr
                    {:tags ["migration" "kg"] :include-expired? true})
   :exclude-tags  (milvus-store/build-filter-expr
                    {:exclude-tags ["carto"] :include-expired? true})
   :with-expiry   (milvus-store/build-filter-expr
                    {:type :note :include-expired? false})
   :empty         (milvus-store/build-filter-expr
                    {:include-expired? true})
   :combined      (milvus-store/build-filter-expr
                    {:type :decision
                     :project-id "hive-mcp"
                     :tags ["architecture"]
                     :exclude-tags ["deprecated"]
                     :include-expired? false})})

;; =============================================================================
;; Golden: helpfulness-map calculation
;; =============================================================================

(golden/deftest-golden helpfulness-map-shapes
  "test/golden/milvus/helpfulness-map.edn"
  {:zero-total    (milvus-store/helpfulness-map
                    {:helpful-count 0 :unhelpful-count 0})
   :all-helpful   (milvus-store/helpfulness-map
                    {:helpful-count 10 :unhelpful-count 0})
   :mixed         (milvus-store/helpfulness-map
                    {:helpful-count 7 :unhelpful-count 3})
   :nil-counts    (milvus-store/helpfulness-map {})})

;; =============================================================================
;; Golden: staleness probability
;; =============================================================================

(golden/deftest-golden staleness-probability-shapes
  "test/golden/milvus/staleness-probability.edn"
  {:fresh     (milvus-store/staleness-probability
                {:staleness-alpha 9 :staleness-beta 1})
   :stale     (milvus-store/staleness-probability
                {:staleness-alpha 1 :staleness-beta 9})
   :balanced  (milvus-store/staleness-probability
                {:staleness-alpha 5 :staleness-beta 5})
   :defaults  (milvus-store/staleness-probability {})})

;; =============================================================================
;; Golden: create-store default config
;; =============================================================================

(golden/deftest-golden-fn create-store-defaults
  "test/golden/milvus/create-store-defaults.edn"
  (fn []
    (let [store (milvus-store/create-store)]
      (select-keys @(.config-atom store) [:host :port :collection-name]))))
