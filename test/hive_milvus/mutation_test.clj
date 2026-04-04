(ns hive-milvus.mutation-test
  "Mutation tests for MilvusMemoryStore — verify tests catch specific bug classes.

   Each mutation simulates a real bug (data loss, wrong conversion, etc.)
   and verifies the contract tests would catch it."
  (:require [clojure.test :refer [deftest testing is]]
            [hive-test.mutation :as mut]
            [hive-milvus.store :as milvus-store]
            [hive-mcp.protocols.memory :as proto]))

;; =============================================================================
;; Mutation: entry->record drops tags (silent data loss)
;; =============================================================================

(mut/deftest-mutation-witness tags-serialization-caught
  hive-milvus.store/tags->str
  (fn [_tags] "[]")  ;; mutant: always returns empty tags
  (fn []
    (let [store (milvus-store/create-store
                  {:host "localhost" :port 19530
                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/connect! store {:host "localhost" :port 19530
                                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/reset-store! store)
          entry {:id (proto/generate-id)
                 :type :convention
                 :content "mutation-test-tags"
                 :tags ["alpha" "beta" "gamma"]
                 :duration :medium}
          _ (proto/add-entry! store entry)
          got (proto/get-entry store (:id entry))]
      (is (= ["alpha" "beta" "gamma"] (:tags got))))))

;; =============================================================================
;; Mutation: record->entry ignores type field (always :note)
;; =============================================================================

(mut/deftest-mutation-witness type-conversion-caught
  hive-milvus.store/record->entry
  (fn [row]
    {:id (:id row) :type :note :content (or (:content row) "")
     :tags [] :duration :medium})  ;; mutant: always returns :note
  (fn []
    (let [store (milvus-store/create-store
                  {:host "localhost" :port 19530
                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/connect! store {:host "localhost" :port 19530
                                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/reset-store! store)
          entry {:id (proto/generate-id)
                 :type :decision
                 :content "mutation-test-type"
                 :tags ["test"]
                 :duration :short}
          _ (proto/add-entry! store entry)
          got (proto/get-entry store (:id entry))]
      (is (= :decision (:type got))))))

;; =============================================================================
;; Mutation: build-filter-expr ignores type filter (returns nil always)
;; =============================================================================

(mut/deftest-mutation-witness filter-expr-caught
  hive-milvus.store/build-filter-expr
  (fn [_opts] nil)  ;; mutant: never filters
  (fn []
    (let [store (milvus-store/create-store
                  {:host "localhost" :port 19530
                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/connect! store {:host "localhost" :port 19530
                                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/reset-store! store)
          _ (proto/add-entry! store
              {:id (proto/generate-id) :type :note
               :content "note-entry" :tags [] :duration :medium})
          _ (proto/add-entry! store
              {:id (proto/generate-id) :type :decision
               :content "decision-entry" :tags [] :duration :medium})
          notes (proto/query-entries store {:type :note :limit 100})]
      ;; If filter is broken, we'd get both entries for a :note query
      (is (every? #(= :note (:type %)) notes)))))

;; =============================================================================
;; Mutation suite: staleness-probability wrong formulas
;; =============================================================================

(mut/deftest-mutations staleness-probability-mutations-caught
  hive-milvus.store/staleness-probability
  [["always-zero"  (fn [_] 0.0)]
   ["always-one"   (fn [_] 1.0)]
   ["inverted"     (fn [e]
                     (let [a (or (:staleness-alpha e) 1)
                           b (or (:staleness-beta e) 1)]
                       (/ (double a) (+ a b))))]]  ;; alpha/(alpha+beta) instead of beta/(alpha+beta)
  (fn []
    (let [store (milvus-store/create-store
                  {:host "localhost" :port 19530
                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/connect! store {:host "localhost" :port 19530
                                   :collection-name "hive-mcp-mutation-test"})
          _ (proto/reset-store! store)
          ;; stale entry: beta=9, alpha=1 → p=0.9
          stale {:id (proto/generate-id) :type :note
                 :content "stale-mut" :tags [] :duration :medium}
          _ (proto/add-entry! store stale)
          _ (proto/update-staleness! store (:id stale) {:beta 9})
          ;; fresh entry: beta=1, alpha=9 → p=0.1
          fresh {:id (proto/generate-id) :type :note
                 :content "fresh-mut" :tags [] :duration :medium}
          _ (proto/add-entry! store fresh)
          _ (proto/update-staleness! store (:id fresh) {:beta 1})
          results (proto/get-stale-entries store 0.5 {})]
      (is (some #(= (:id stale) (:id %)) results))
      (is (not (some #(= (:id fresh) (:id %)) results))))))
