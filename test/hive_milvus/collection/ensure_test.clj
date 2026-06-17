(ns hive-milvus.collection.ensure-test
  "Tests for the dim-check policy in
   `hive-milvus.collection.ensure/ensure-with-check`.

   No Milvus required — exercises the in-process dim registry directly
   via the public test helper (`reset-registry!`). Rejecting a
   conflicting-dim re-ensure surfaces `:collection/dim-mismatch`,
   which the resilience classifier converts to `:err/schema-mismatch`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-dsl.result :as r]
            [hive-milvus.collection.ensure :as ensure]))

;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(use-fixtures :each (fn [t] (ensure/reset-registry!) (t)))

(defn- ref [name dim]
  {:coll/name name :coll/dim dim :coll/sunset? false})

(deftest matching-redims-pass-through
  (testing "Same dim re-registration is idempotent"
    ;; Pre-seed registry to skip Milvus call.
    (reset! @(resolve 'hive-milvus.collection.ensure/dim-registry)
            {"x_1024d" 1024})
    (let [result (ensure/ensure-with-check (ref "x_1024d" 1024))]
      (is (r/ok? result))
      (is (= 1024 (-> result :ok :coll/dim))))))

(deftest conflicting-dim-rejected
  (testing "Same name, different dim → :collection/dim-mismatch"
    (reset! @(resolve 'hive-milvus.collection.ensure/dim-registry)
            {"x_1024d" 1024})
    (let [result (ensure/ensure-with-check (ref "x_1024d" 4096))]
      (is (r/err? result))
      (is (= :collection/dim-mismatch (:error result)))
      (is (= 1024 (:existing-dim result)))
      (is (= 4096 (:requested-dim result))))))
