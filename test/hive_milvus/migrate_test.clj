(ns hive-milvus.migrate-test
  "Tests for pure portions of hive-milvus.migrate — principally
   `rows->entries`, extracted from `read-all-entries` so it can be
   tested without a live SQLite database."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [hive-test.properties :refer [defprop-total]]
            [hive-milvus.migrate :as migrate]))

;; =============================================================================
;; Unit tests for rows->entries
;; =============================================================================
;;
;; Shape of a row (matches what rs->rows materializes):
;;   {:embedding-id "id-1" :key "content" :value "hello"}
;;
;; Each entry in the output is the result of (metadata->entry id meta),
;; which produces a map keyed by :id plus the Chroma-derived fields.

(defn- row [eid k v]
  {:embedding-id eid :key k :value v})

(deftest empty-rows
  (testing "empty row vec → empty entry vec"
    (is (= [] (migrate/rows->entries [])))))

(deftest single-row-single-kv
  (testing "one row with one metadata pair → one entry carrying that pair"
    (let [[entry :as out] (migrate/rows->entries [(row "id-1" "content" "hello")])]
      (is (= 1 (count out)))
      (is (= "id-1"  (:id entry)))
      (is (= "hello" (:content entry))))))

(deftest two-rows-same-id-merge
  (testing "two rows, same embedding-id, different keys → one merged entry"
    (let [rows [(row "id-1" "content"      "hello")
                (row "id-1" "content-hash" "abc123")]
          [entry :as out] (migrate/rows->entries rows)]
      (is (= 1 (count out)))
      (is (= "id-1"   (:id entry)))
      (is (= "hello"  (:content entry)))
      (is (= "abc123" (:content-hash entry))))))

(deftest three-rows-A-A-B-yields-two
  (testing "A A B rows → 2 entries (transition flushes A; end flushes B)"
    (let [rows [(row "id-A" "content"      "alpha")
                (row "id-A" "content-hash" "h1")
                (row "id-B" "content"      "beta")]
          out  (migrate/rows->entries rows)]
      (is (= 2 (count out)))
      (is (= ["id-A" "id-B"] (mapv :id out)))
      (is (= "alpha" (:content (first out))))
      (is (= "h1"    (:content-hash (first out))))
      (is (= "beta"  (:content (second out)))))))

(deftest four-rows-A-B-A-treats-as-three-entries
  (testing "non-contiguous repeat of A (A B A) → 3 entries, since rows->entries
            groups only consecutive runs. Callers must pre-sort (SQL ORDER BY does)."
    (let [rows [(row "id-A" "content" "1")
                (row "id-B" "content" "2")
                (row "id-A" "content" "3")]
          out  (migrate/rows->entries rows)]
      (is (= 3 (count out)))
      (is (= ["id-A" "id-B" "id-A"] (mapv :id out))))))

;; =============================================================================
;; Property: totality + distinct-id count
;; =============================================================================

(def ^:private gen-id
  (gen/elements ["id-A" "id-B" "id-C" "id-D"]))

(def ^:private gen-key
  (gen/elements ["content" "content-hash" "created" "updated" "tags" "type"]))

(def ^:private gen-row-map
  (gen/fmap (fn [[eid k v]] {:embedding-id eid :key k :value v})
            (gen/tuple gen-id gen-key gen/string-alphanumeric)))

(def ^:private gen-sorted-rows
  ;; Sort to match SQL ORDER BY embedding_id precondition, so the
  ;; distinct-id count equals (count entries).
  (gen/fmap (fn [rows] (vec (sort-by :embedding-id rows)))
            (gen/vector gen-row-map 0 20)))

(defprop-total rows->entries-total
  migrate/rows->entries
  gen-sorted-rows
  {:num-tests 200
   :pred (fn [_] true)})

(defprop-total rows->entries-count-matches-distinct-ids
  (fn [rows]
    (= (count (migrate/rows->entries rows))
       (count (into #{} (map :embedding-id) rows))))
  gen-sorted-rows
  {:num-tests 200
   :pred true?})
