(ns hive-milvus.consistency-test
  "Golden, property, and mutation tests for Milvus consistency fixes.

   Tests the ensure-collection! load-on-reconnect behavior and
   the strong consistency enforcement on get/query operations.

   Integration tests require a live Milvus instance."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [hive-test.golden :as golden]
            [hive-test.properties :as props]
            [hive-test.mutation :as mut]
            [hive-milvus.store :as milvus-store]
            [hive-mcp.protocols.memory :as proto]
            [milvus-clj.api :as milvus]))

;; =============================================================================
;; Shared fixture
;; =============================================================================

(def ^:private test-store (atom nil))
(def ^:private test-collection "hive-mcp-consistency-test")

(defn setup-milvus-store [f]
  (let [store (milvus-store/create-store
                {:host "localhost" :port 19530
                 :collection-name test-collection})]
    (proto/connect! store {:host "localhost" :port 19530
                           :collection-name test-collection})
    (reset! test-store store)
    (try (f)
         (finally
           (proto/reset-store! store)
           (proto/disconnect! store)))))

(use-fixtures :once setup-milvus-store)

(defn- fresh-store []
  (proto/reset-store! @test-store)
  ;; Re-ensure collection after reset
  (proto/connect! @test-store {:host "localhost" :port 19530
                               :collection-name test-collection})
  @test-store)

;; =============================================================================
;; Golden: ensure-collection! loads existing collection
;; =============================================================================

(golden/deftest-golden-fn ensure-collection-load-shape
  "test/golden/hive-milvus/ensure-collection-load.edn"
  (fn []
    ;; After connect!, collection should exist and be loaded
    (let [store @test-store
          health (proto/health-check store)]
      {:healthy?  (:healthy? health)
       :backend   (:backend health)
       :has-errors (boolean (seq (:errors health)))})))

;; =============================================================================
;; Golden: read-after-write works with strong consistency
;; =============================================================================

(golden/deftest-golden-fn read-after-write-shape
  "test/golden/hive-milvus/read-after-write.edn"
  (fn []
    (let [store (fresh-store)
          entry {:id (proto/generate-id)
                 :type :note
                 :content "consistency-golden-test"
                 :tags ["consistency" "golden"]
                 :duration :short}
          entry-id (proto/add-entry! store entry)
          got (proto/get-entry store entry-id)]
      {:wrote-id    entry-id
       :read-back?  (some? got)
       :type-match? (= :note (:type got))
       :tags-match? (= ["consistency" "golden"] (:tags got))})))

;; =============================================================================
;; Property: add-then-get roundtrip always succeeds (strong consistency)
;; =============================================================================

(defspec prop-add-get-roundtrip 10
  (prop/for-all [suffix gen/string-alphanumeric]
    (let [store (fresh-store)
          entry {:id (proto/generate-id)
                 :type :note
                 :content (str "prop-roundtrip-" suffix)
                 :tags ["prop-test"]
                 :duration :medium}
          entry-id (proto/add-entry! store entry)
          got (proto/get-entry store entry-id)]
      (and (some? got)
           (= entry-id (:id got))
           (= :note (:type got))))))

;; =============================================================================
;; Property: query-entries immediately sees added entries (strong consistency)
;; =============================================================================

(defspec prop-add-query-immediate-visibility 10
  (prop/for-all [n (gen/choose 1 3)]
    (let [store (fresh-store)
          tag (str "vis-" (random-uuid))
          ids (mapv (fn [i]
                      (proto/add-entry! store
                        {:id (proto/generate-id)
                         :type :note
                         :content (str "visibility-" i)
                         :tags [tag]
                         :duration :medium}))
                    (range n))
          results (proto/query-entries store {:type :note :limit 100})]
      ;; All added entries should be visible immediately
      (let [result-ids (set (map :id results))]
        (every? #(contains? result-ids %) ids)))))

;; =============================================================================
;; Mutation: get-entry-by-id without strong consistency → read-after-write fails
;; (This is the ORIGINAL BUG — verifies our fix catches it)
;; =============================================================================

(mut/deftest-mutation-witness get-entry-consistency-caught
  milvus-clj.api/get
  ;; Mutant: remove consistency-level (the original broken behavior)
  (fn [collection-name ids & {:keys [include]}]
    (future
      (let [client @(resolve 'milvus-clj.api/client-atom)
            client (deref client)]
        ;; This would be the original get without consistency — we can't
        ;; easily replicate without internals, so we simulate by returning
        ;; empty (which is what bounded consistency does on race)
        [])))
  (fn []
    (let [store (fresh-store)
          entry {:id (proto/generate-id)
                 :type :note
                 :content "mutation-consistency-test"
                 :tags ["mutation"]
                 :duration :short}
          entry-id (proto/add-entry! store entry)
          got (proto/get-entry store entry-id)]
      (is (some? got) "get-entry must return the entry after add")
      (is (= entry-id (:id got))))))

;; =============================================================================
;; Mutation: query-entries without strong consistency → list misses entries
;; =============================================================================

(mut/deftest-mutation-witness query-entries-consistency-caught
  milvus-clj.api/query-scalar
  ;; Mutant: always returns empty results (simulates bounded consistency miss)
  (fn [_coll-name _opts]
    (future []))
  (fn []
    (let [store (fresh-store)
          _ (proto/add-entry! store
              {:id (proto/generate-id)
               :type :note
               :content "query-mutation-test"
               :tags ["qmt"]
               :duration :medium})
          results (proto/query-entries store {:type :note :limit 100})]
      (is (pos? (count results)) "query-entries must find added entries"))))
