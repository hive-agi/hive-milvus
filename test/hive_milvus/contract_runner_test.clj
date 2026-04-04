(ns hive-milvus.contract-runner-test
  "Runs the backend-agnostic contract tests against MilvusMemoryStore.

   Binds contract/*store-factory* so the parameterized tests in
   contract-test execute against the Milvus implementation.

   Requires a running Milvus instance (k8s port-forward or local)."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-mcp.memory.store.contract-test :as contract]
            [hive-milvus.store :as milvus-store]
            [hive-mcp.protocols.memory :as proto]))

;; =============================================================================
;; Factory Binding
;; =============================================================================

(defn bind-milvus-factory
  "Fixture: bind *store-factory* to MilvusMemoryStore for the duration of tests."
  [f]
  (binding [contract/*store-factory*
            #(let [store (milvus-store/create-store
                           {:host "localhost" :port 19530
                            :collection-name "hive-mcp-memory-test"})]
               (proto/connect! store {:host "localhost" :port 19530
                                      :collection-name "hive-mcp-memory-test"})
               store)]
    (f)))

(use-fixtures :each bind-milvus-factory)

;; =============================================================================
;; Protocol Satisfaction
;; =============================================================================

(deftest milvus-satisfies-all-protocols
  (let [store (milvus-store/create-store)]
    (testing "MilvusMemoryStore satisfies IMemoryStore"
      (is (satisfies? proto/IMemoryStore store)))
    (testing "MilvusMemoryStore satisfies IMemoryStoreWithAnalytics"
      (is (satisfies? proto/IMemoryStoreWithAnalytics store)))
    (testing "MilvusMemoryStore satisfies IMemoryStoreWithStaleness"
      (is (satisfies? proto/IMemoryStoreWithStaleness store)))))

;; =============================================================================
;; Contract Test Invocations — IMemoryStore: Connection Lifecycle
;; =============================================================================

(deftest milvus-lifecycle-connected
  (contract/test-lifecycle-connected))

(deftest milvus-lifecycle-health-check-shape
  (contract/test-lifecycle-health-check-shape))

(deftest milvus-lifecycle-store-status-shape
  (contract/test-lifecycle-store-status-shape))

(deftest milvus-lifecycle-connect-then-connected
  (contract/test-lifecycle-connect-then-connected))

(deftest milvus-disconnect-shape
  (contract/test-disconnect-shape))

;; =============================================================================
;; Contract Test Invocations — IMemoryStore: CRUD
;; =============================================================================

(deftest milvus-add-get-roundtrip
  (contract/test-add-get-roundtrip))

(deftest milvus-add-delete-get
  (contract/test-add-delete-get))

(deftest milvus-add-update-get
  (contract/test-add-update-get))

(deftest milvus-add-delete-count-invariant
  (contract/test-add-delete-count-invariant))

;; =============================================================================
;; Contract Test Invocations — IMemoryStore: Query & Search
;; =============================================================================

(deftest milvus-query-entries-by-type
  (contract/test-query-entries-by-type))

(deftest milvus-search-similar-behavioral
  (contract/test-search-similar-behavioral))

;; =============================================================================
;; Contract Test Invocations — IMemoryStore: Duplicate Detection
;; =============================================================================

(deftest milvus-find-duplicate-same-content
  (contract/test-find-duplicate-same-content))

(deftest milvus-find-duplicate-different-content
  (contract/test-find-duplicate-different-content))

;; =============================================================================
;; Contract Test Invocations — IMemoryStore: Expiration
;; =============================================================================

(deftest milvus-expiration-cleanup
  (contract/test-expiration-cleanup))

(deftest milvus-cleanup-expired-idempotent
  (contract/test-cleanup-expired-idempotent))

(deftest milvus-entries-expiring-soon
  (contract/test-entries-expiring-soon))

;; =============================================================================
;; Contract Test Invocations — IMemoryStore: Reset
;; =============================================================================

(deftest milvus-reset-store-idempotent
  (contract/test-reset-store-idempotent))

;; =============================================================================
;; Contract Test Invocations — IMemoryStoreWithAnalytics
;; =============================================================================

(deftest milvus-analytics-log-access
  (contract/test-analytics-log-access))

(deftest milvus-analytics-record-feedback
  (contract/test-analytics-record-feedback))

(deftest milvus-analytics-helpfulness-ratio
  (contract/test-analytics-helpfulness-ratio))

;; =============================================================================
;; Contract Test Invocations — IMemoryStoreWithStaleness
;; =============================================================================

(deftest milvus-staleness-update
  (contract/test-staleness-update))

(deftest milvus-staleness-get-stale-entries
  (contract/test-staleness-get-stale-entries))

(deftest milvus-staleness-propagate
  (contract/test-staleness-propagate))
