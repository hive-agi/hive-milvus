(ns hive-milvus.store.page-port-test
  "IMemoryStorePage / IMemoryStoreCount over a stubbed Milvus. milvus-clj and
   the collection lookup are rebound; nothing touches a live collection."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hive-milvus.store :as store]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.schema :as schema]
            [hive-spi.memory.ports :as ports]
            [milvus-clj.api :as milvus]))

(defn- rec [id h]
  {:id id :type "note" :tags "[]" :content id :content_hash h
   :created "2026-07-12T00:00:00Z" :updated "2026-07-12T00:00:00Z"
   :duration "medium" :expires "" :access_count 0 :project_id "p"})

(defn- fake-fetch
  "FETCH over `by-coll` {coll [rows]}: honours `id > \"x\"`, ascending id, limit."
  [by-coll]
  (fn [coll filt limit]
    (let [after (second (re-find #"id > \"([^\"]*)\"" filt))]
      (->> (get by-coll coll)
           (filter #(or (nil? after) (pos? (compare (:id %) after))))
           (sort-by :id)
           (take limit)
           vec))))

(defn- walk [fetch colls base limit keep?]
  (loop [cursor nil, rows [], pages 0]
    (let [r (entries/page-step fetch colls base limit keep? cursor)
          rows (into rows (:rows r))]
      (if (and (:next-cursor r) (< pages 100))
        (recur (:next-cursor r) rows (inc pages))
        {:rows rows :pages (inc pages)}))))

(deftest a-walk-visits-every-row-once-across-collections-test
  (let [data {"a" (mapv #(rec (format "a%03d" %) "x") (range 7))
              "b" (mapv #(rec (format "b%03d" %) "x") (range 4))
              "c" []}
        {:keys [rows]} (walk (fake-fetch data) ["a" "b" "c"] nil 3 (constantly true))]
    (is (= (mapv :id (concat (data "a") (data "b"))) (mapv :id rows)))))

(deftest a-page-shorter-than-the-limit-is-not-the-end-test
  (let [data {"a" [(rec "a1" "x")] "b" [(rec "b1" "x")]}
        r (entries/page-step (fake-fetch data) ["a" "b"] nil 10 (constantly true) nil)]
    (is (= ["a1"] (mapv :id (:rows r))))
    (is (some? (:next-cursor r)) "the next collection is still to walk")))

(deftest the-walk-ends-with-a-nil-cursor-test
  (let [r (entries/page-step (fake-fetch {"a" [(rec "a1" "x")]}) ["a"] nil 10 (constantly true) nil)]
    (is (nil? (:next-cursor r)))))

(deftest a-filtered-page-still-advances-test
  (testing "rows the prefix test drops do not stall the cursor"
    (let [data {"a" (mapv #(rec (format "a%03d" %) (if (even? %) "hk1:k:z" "other")) (range 9))}
          keep? #(schema/hash-prefix-match? "hk1:k:" %)
          {:keys [rows]} (walk (fake-fetch data) ["a"] nil 2 keep?)]
      (is (= 5 (count rows)))
      (is (every? #(str/starts-with? (:content_hash %) "hk1:k:") rows)))))

(deftest an-unordered-page-raises-rather-than-skipping-rows-test
  (is (thrown? clojure.lang.ExceptionInfo
               (entries/page-step (fn [_ _ _] [(rec "b" "x") (rec "a" "x")])
                                  ["a"] nil 2 (constantly true) nil))))

(deftest a-failing-collection-raises-not-an-empty-page-test
  (let [timeout (ex-info "milvus query-scalar timed out after 5000ms" {:milvus/timeout true})]
    (is (thrown? clojure.lang.ExceptionInfo
                 (entries/page-step (fn [_ _ _] (throw timeout)) ["a"] nil 2 (constantly true) nil)))))

(deftest content-hash-prefix-becomes-a-like-clause-test
  (let [f (schema/build-filter-expr {:content-hash-prefix "hk1:abc:" :include-expired? true})]
    (is (= "content_hash like \"hk1:abc:%\"" f)))
  (testing "wildcards and quotes cannot widen the match"
    (is (= "hk1:a" (schema/hash-prefix-literal "hk1:a%b")))
    (is (= "a\\\"b" (schema/hash-prefix-literal "a\"b")))
    (is (nil? (schema/hash-prefix-literal "")))
    (is (nil? (schema/hash-prefix-literal "%x"))))
  (is (nil? (schema/build-filter-expr {:include-expired? true})))
  (is (schema/hash-prefix-match? "hk1:k:" {:content-hash "hk1:k:9"}))
  (is (not (schema/hash-prefix-match? "hk1:k:" {:content_hash "hk1:j:9"}))))

(deftest the-store-implements-the-page-and-count-ports-test
  (let [s (store/create-store {})]
    (is (satisfies? ports/IMemoryStorePage s))
    (is (satisfies? ports/IMemoryStoreCount s))))

(deftest query-page-reads-through-milvus-with-a-prefix-filter-test
  (let [asked (atom [])
        rows  [(rec "a1" "hk1:k:1") (rec "a2" "other") (rec "a3" "hk1:k:3")]]
    (with-redefs [lookup/known-collections (fn [_] ["a"])
                  milvus/query-scalar (fn [coll q] (swap! asked conj [coll q]) (delay rows))
                  hive-milvus.resilience.retry/ensure-live! (fn [_] true)
                  hive-milvus.circuit/check (fn [] nil)]
      (let [r (entries/query-page (atom {}) {:content-hash-prefix "hk1:k:" :limit 10
                                             :output-fields ["id" "content_hash"]
                                             :include-expired? true})]
        (is (= ["a1" "a3"] (mapv :id (:rows r))))
        (is (every? #(= #{:id :content-hash} (set (keys %))) (:rows r)))
        (is (nil? (:next-cursor r)))
        (let [[coll q] (first @asked)]
          (is (= "a" coll))
          (is (re-find #"content_hash like \"hk1:k:%\"" (:filter q)))
          (is (some #{"content_hash"} (:output-fields q))))))))

(deftest count-entries-sums-server-side-counts-and-never-reports-failure-as-zero-test
  (with-redefs [lookup/known-collections (fn [_] ["a" "b"])
                hive-milvus.resilience.retry/ensure-live! (fn [_] true)
                hive-milvus.circuit/check (fn [] nil)]
    (with-redefs [milvus/query-scalar (fn [coll _] (delay [{(keyword "count(*)") (if (= coll "a") 3 4)}]))]
      (is (= 7 (entries/count-entries (atom {}) {:include-expired? true}))))
    (with-redefs [milvus/query-scalar (fn [_ _] (delay [{}]))]
      (is (thrown? Exception (entries/count-entries (atom {}) {:include-expired? true}))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (entries/count-entries (atom {}) {:content-hash-prefix "a%b"})))))
