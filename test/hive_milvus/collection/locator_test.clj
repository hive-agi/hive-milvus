(ns hive-milvus.collection.locator-test
  "Tests for the L2 `DefaultLocator`. Exercises sunset-aware fan-out
   without touching Milvus."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-milvus.collection.locator :as locator]
            [hive-milvus.collection.protocol :as proto]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(defn- mk-locator [sunset]
  (locator/make-locator (constantly {:sunset sunset
                                     :base-collection-name "hive-mcp-memory"})))

(deftest collection-for-uses-spec-dim
  (testing "spec dim drives ref name + dim"
    (let [l (mk-locator #{})
          ref (proto/collection-for l {:provider/dim 1024})]
      (is (= "hive_mcp_memory_1024d" (:coll/name ref)))
      (is (= 1024 (:coll/dim ref))))))

(deftest active-excludes-sunset
  (testing "active-collections drops sunset entries"
    (let [l (mk-locator #{"hive_mcp_memory"})
          actives (proto/active-collections l)
          names (set (map :coll/name actives))]
      (is (not (contains? names "hive_mcp_memory")))
      (is (contains? names "hive_mcp_memory_1024d"))
      (is (contains? names "hive_mcp_memory_4096d")))))

(deftest known-includes-sunset
  (testing "known-collections keeps sunset (for read fan-out during drain)"
    (let [l (mk-locator #{"hive_mcp_memory"})
          known (proto/known-collections l)
          names (set (map :coll/name known))]
      (is (contains? names "hive_mcp_memory"))
      (is (contains? names "hive_mcp_memory_1024d"))
      (is (contains? names "hive_mcp_memory_4096d"))
      ;; Sunset flag set on legacy
      (is (true? (->> known
                      (filter #(= "hive_mcp_memory" (:coll/name %)))
                      first :coll/sunset?))))))
