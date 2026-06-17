(ns hive-milvus.collection.naming-test
  "Pure unit + property tests for `hive-milvus.collection.naming`."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-milvus.collection.naming :as naming]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(deftest legacy-768-keeps-base-name
  (testing "768 → \"hive-mcp-memory\" (legacy, pre-multi-dim)"
    (is (= "hive-mcp-memory" (naming/chroma-name 768)))))

(deftest non-legacy-dims-get-suffix
  (testing "1024 → \"hive-mcp-memory-1024d\""
    (is (= "hive-mcp-memory-1024d" (naming/chroma-name 1024))))
  (testing "4096 → \"hive-mcp-memory-4096d\""
    (is (= "hive-mcp-memory-4096d" (naming/chroma-name 4096)))))

(deftest milvus-name-converts-hyphen-to-underscore
  (testing "Chroma → Milvus form"
    (is (= "hive_mcp_memory"        (naming/milvus-name "hive-mcp-memory")))
    (is (= "hive_mcp_memory_1024d"  (naming/milvus-name "hive-mcp-memory-1024d")))
    (is (= "hive_mcp_memory_4096d"  (naming/milvus-name "hive-mcp-memory-4096d")))))

(deftest milvus-name-idempotent
  (testing "name already in Milvus form passes through"
    (is (= "hive_mcp_memory_1024d" (naming/milvus-name "hive_mcp_memory_1024d")))))

(deftest invalid-dim-throws
  (testing "non-positive dim is a programming error"
    (is (thrown? Exception (naming/chroma-name 0)))
    (is (thrown? Exception (naming/chroma-name -1)))
    (is (thrown? Exception (naming/chroma-name nil)))))

(deftest ref-for-dim-shape
  (testing "ref-for-dim builds a complete CollectionRef"
    (let [ref (naming/ref-for-dim 1024)]
      (is (= "hive_mcp_memory_1024d" (:coll/name ref)))
      (is (= 1024                    (:coll/dim ref)))
      (is (false?                    (:coll/sunset? ref))))))

;; ---------------------------------------------------------------------------
;; Property: every positive dim produces a parseable CollectionRef whose
;; dim and chroma-name agree (the structural invariant locator depends on).
;; ---------------------------------------------------------------------------

(defspec ref-for-dim-self-consistent 200
  (prop/for-all [dim (gen/such-that pos? gen/small-integer)]
    (let [ref (naming/ref-for-dim dim)]
      (and (= dim (:coll/dim ref))
           (string? (:coll/name ref))
           (or (= "hive-mcp-memory" (:coll/chroma ref))
               (.contains ^String (:coll/chroma ref) (str dim "d")))))))
