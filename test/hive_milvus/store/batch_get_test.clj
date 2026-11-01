(ns hive-milvus.store.batch-get-test
  "A batch get is read in pages, and a page no collection could answer makes
   the result an error Result - never a short vector that reads as absence.
   No live Milvus: `gather` takes its fetch port as an argument."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-milvus.store.batch :as batch]))

(defn- ids [n] (mapv #(str "id-" %) (range n)))

(defn- store-fetch
  "A fetch port over {coll #{ids}} that records every page it is asked."
  [held asked]
  (fn [coll page]
    (swap! asked conj [coll (count page)])
    (into [] (comp (filter (get held coll #{})) (map (fn [id] {:id id :coll coll}))) page)))

(deftest a-large-ask-is-paged-and-answers-every-row-test
  (let [asked (atom [])
        all   (ids 704)
        res   (batch/gather (store-fetch {"a" (set all)} asked) ["a"] all)]
    (is (vector? res))
    (is (= 704 (count res)))
    (is (every? #(<= % batch/page-size) (map second @asked)))
    (is (= (long (Math/ceil (/ 704 batch/page-size))) (count @asked)))))

(deftest first-collection-wins-a-duplicate-id-test
  (let [res (batch/gather (store-fetch {"a" #{"id-0"} "b" #{"id-0" "id-1"}} (atom []))
                          ["a" "b"] (ids 2))]
    (is (= {"id-0" "a" "id-1" "b"} (into {} (map (juxt :id :coll)) res)))))

(deftest a-timed-out-page-is-an-error-result-not-absence-test
  (let [timed-out (ex-info "milvus get timed out after 5000ms" {:milvus/timeout true})
        fetch     (fn [coll page]
                    (if (= "id-50" (first page))
                      (throw timed-out)
                      (mapv (fn [id] {:id id :coll coll}) page)))
        res       (batch/gather fetch ["a"] (ids 120))]
    (is (r/err? res))
    (is (= :milvus/read-incomplete (:error res)))
    (is (= [{:collection "a" :message "milvus get timed out after 5000ms" :ids 50}]
           (:failed res)))
    (is (= 70 (count (:partial res))))))

(deftest a-missing-collection-answers-none-test
  (let [missing (ex-info "collection not found[collection=hive_mcp_memory_1024d]" {})
        fetch   (fn [coll page]
                  (if (= "gone" coll)
                    (throw missing)
                    (mapv (fn [id] {:id id}) page)))]
    (testing "a never-created collection is an answer, not an outage"
      (is (= 3 (count (batch/gather fetch ["gone" "a"] (ids 3))))))))

(deftest page-deadline-scales-with-the-page-test
  (is (< (batch/page-deadline-ms 1) (batch/page-deadline-ms batch/page-size))))
