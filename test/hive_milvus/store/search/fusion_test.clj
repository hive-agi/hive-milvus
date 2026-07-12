(ns hive-milvus.store.search.fusion-test
  "Trifecta for cross-space fusion. Property + mutation facets are synthesized
   from the malli schemas."
  (:require [clojure.test :refer [deftest testing is]]
            [hive-schemas.test :as hst]
            [hive-test.mutation :as mut]
            [hive-milvus.store.search.fusion :as fusion]))

;; ============================================================================
;; Schema-synthesized
;; ============================================================================

;; THE THEOREM: ranking a space by distance is order-preserving and total.
(hst/deftrifecta-from-schema rank-hits-orders-by-distance
  hive-milvus.store.search.fusion/rank-hits
  {:in  [:sequential fusion/Hit]
   :out [:sequential [:map [:id :string] [:rank [:int {:min 1}]]]]
   :rel (fn [in out]
          (and (= (count in) (count out))
               ;; ranks are 1..n, no gaps, no duplicates
               (= (set (map :rank out)) (set (range 1 (inc (count in)))))
               ;; and a nearer hit never ranks worse than a farther one
               (every? (fn [[a b]] (<= (:distance a) (:distance b)))
                       (partition 2 1 out))))
   :mutation false
   :num-tests 300})

(mut/deftest-mutations rank-hits-mutants-are-caught
  hive-milvus.store.search.fusion/rank-hits
  [["ranks by FARTHEST first — the sign of the comparison"
    (fn [hits] (->> hits (sort-by :distance >) (map-indexed (fn [i h] (assoc h :rank (inc i)))) vec))]
   ["leaves the input order alone and just numbers it"
    (fn [hits] (vec (map-indexed (fn [i h] (assoc h :rank (inc i))) hits)))]
   ["ranks from 0, so rrf-score divides by a different k"
    (fn [hits] (->> hits (sort-by :distance) (map-indexed (fn [i h] (assoc h :rank i))) vec))]]
  (fn []
    (let [hits [{:id "far" :distance 9.0} {:id "near" :distance 0.1} {:id "mid" :distance 1.0}]
          out  (fusion/rank-hits hits)]
      (is (= "near" (:id (first out))) "nearest ranks first")
      (is (= 1 (:rank (first out))) "ranks are 1-based")
      (is (= ["near" "mid" "far"] (mapv :id out))))))

;; ============================================================================
;; The domain rule — distance is not comparable across spaces; rank is
;; ============================================================================

(deftest fusion-is-invariant-to-a-change-of-distance-units
  (testing "rescaling one space's distances must not change the fused order"
    (let [space-a {:space :a :hits [{:id "x" :distance 0.1} {:id "y" :distance 0.2}]}
          space-b {:space :b :hits [{:id "z" :distance 0.15} {:id "x" :distance 0.30}]}
          inflate (fn [sh] (update sh :hits #(mapv (fn [h] (update h :distance * 1000.0)) %)))
          normal  (fusion/fuse [space-a space-b] 10)
          skewed  (fusion/fuse [space-a (inflate space-b)] 10)]
      (is (= (mapv :id normal) (mapv :id skewed))
          "same order, though space :b's numbers are 1000x larger")
      ;; and the naive approach WOULD have been fooled:
      (let [naive (->> (concat (:hits space-a) (:hits (inflate space-b)))
                       (sort-by :distance) (mapv :id))]
        (is (not= (mapv :id skewed) (take 2 naive))
            "sorting raw distances across spaces gives a different, unit-dependent answer")))))

(deftest an-entry-found-in-two-spaces-appears-once-and-is-promoted
  (testing "agreement across spaces raises a hit; it is not returned twice"
    (let [results [{:space :old :hits [{:id "both" :distance 0.5} {:id "old-only" :distance 0.1}]}
                   {:space :new :hits [{:id "both" :distance 0.5} {:id "new-only" :distance 0.1}]}]
          fused   (fusion/fuse results 10)]
      (is (= 3 (count fused)) "three distinct ids, not four rows")
      (is (= 1 (count (filter #(= "both" (:id %)) fused))) "deduplicated")
      (is (= "both" (:id (first fused)))
          "found by both spaces, so it outranks either space's own top hit")
      (is (= [:old :new] (:spaces (first fused)))))))

(deftest a-single-space-needs-no-fusion
  (testing "one space: the fused order IS the distance order"
    (let [results [{:space :only :hits [{:id "c" :distance 3.0}
                                        {:id "a" :distance 1.0}
                                        {:id "b" :distance 2.0}]}]]
      (is (true? (fusion/single-space? results)))
      (is (= ["a" "b" "c"] (mapv :id (fusion/fuse results 10)))))))

(deftest fuse-respects-the-limit
  (let [results [{:space :a :hits (mapv (fn [i] {:id (str i) :distance (double i)}) (range 50))}]]
    (is (= 5 (count (fusion/fuse results 5))))))

(deftest fusing-nothing-yields-nothing
  (is (= [] (fusion/fuse [] 10)))
  (is (= [] (fusion/fuse [{:space :a :hits []}] 10))))
