(ns hive-milvus.relocate.commit-page-test
  "`pipeline/commit-page` writes a page with ONE upsert per target collection
   and keeps per-id failure attribution when a batch fails.

   Driven through its ports only: a stub IRecordWriter that counts calls and
   can refuse multi-record batches or named records, and a stub
   ISourceDisposition that records which ids it was handed. No Milvus, no
   embedder, no with-redefs."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.result :as r]
            [hive-milvus.relocate :as relocate]
            [hive-milvus.relocate.pipeline :as p]
            [hive-test.trifecta :refer [deftrifecta]]))

;; ============================================================
;; Ports
;; ============================================================

(defrecord StubWriter [calls fail-batch? bad-ids]
  p/IRecordWriter
  (-write-records [_ coll records]
    (swap! calls conj [coll (mapv :id records)])
    (cond
      (and fail-batch? (> (count records) 1))
      (r/err :boundary/milvus-write-failed {:coll coll :n (count records)})

      (some (comp bad-ids :id) records)
      (r/err :boundary/milvus-write-failed {:coll coll :ids (mapv :id records)})

      :else
      (r/ok (count records)))))

(defrecord RecordingDisposition [seen]
  p/ISourceDisposition
  (-after-write [_ bundle] (swap! seen conj (:id bundle)) (r/ok bundle))
  (-describe-disposition [_] :move))

;; ============================================================
;; Harness
;; ============================================================

(defn- ready [id coll]
  [id (r/ok {:prepare/status :ready
             :bundle {:id id :src-coll "old" :target-coll coll
                      :record {:id id :embedding [0.5]}}})])

(defn- prepared-page
  "[id Result] pairs from a page spec: each row is [id kind coll]."
  [rows]
  (mapv (fn [[id kind coll]]
          (case kind
            :ready (ready id coll)
            :no-op [id (r/ok {:prepare/status :no-op
                              :result {:placed? false :from coll :to coll :id id}})]
            :err   [id (r/err :embedder/embed-failed {:id id})]))
        rows))

(defn commit-outcome
  "Run `commit-page` over a page spec against the stub ports.
   Returns the batched-call count per collection, which ids the disposition
   saw, and each id's outcome as :placed / :no-op / the error keyword."
  [{:keys [rows fail-batch? bad-ids]}]
  (let [calls  (atom [])
        seen   (atom [])
        placed (p/commit-page (->StubWriter calls fail-batch? (set bad-ids))
                              (->RecordingDisposition seen)
                              (prepared-page rows))]
    {:batch-calls (->> @calls (filter (fn [[_ ids]] (> (count ids) 1)))
                       (map first) frequencies)
     :disposed    (set @seen)
     :outcomes    (into (sorted-map)
                        (map (fn [[id res]]
                               [id (cond
                                     (and (r/ok? res) (:placed? (:ok res))) :placed
                                     (r/ok? res)                            :no-op
                                     :else                                  (:error res))]))
                        placed)}))

;; ============================================================
;; Invariant
;; ============================================================

(defn- attributed?
  "Every id has an outcome; only placed ids reach the disposition (delete
   after write); with no failures there is at most one multi-record call per
   collection."
  [{:keys [rows bad-ids fail-batch?]} {:keys [batch-calls disposed outcomes]}]
  (and (= (set (map first rows)) (set (keys outcomes)))
       (= disposed (set (keep (fn [[id o]] (when (= :placed o) id)) outcomes)))
       (every? (fn [id] (not= :placed (get outcomes id))) bad-ids)
       (or fail-batch? (seq bad-ids) (every? #(= 1 %) (vals batch-calls)))))

(def ^:private gen-page
  (gen/let [n     (gen/choose 1 12)
            kinds (gen/vector (gen/frequency [[6 (gen/return :ready)]
                                              [1 (gen/return :no-op)]
                                              [1 (gen/return :err)]]) n)
            colls (gen/vector (gen/elements ["c1024" "c4096"]) n)
            fail? gen/boolean
            bad   (gen/vector (gen/choose 0 11) 0 2)]
    (let [rows (mapv (fn [i k c] [(str "id" i) k c]) (range n) kinds colls)]
      {:rows rows :fail-batch? fail? :bad-ids (mapv #(str "id" %) bad)})))

(defn commit-checked
  "Harness for the property facet: the outcome plus whether it holds."
  [input]
  (let [out (commit-outcome input)]
    (assoc out :ok? (attributed? input out))))

;; ============================================================
;; Trifecta
;; ============================================================

(deftrifecta commit-page-batches-and-attributes
  hive-milvus.relocate.commit-page-test/commit-checked
  {:golden-path "test/golden/milvus/trifecta-commit-page.edn"
   :cases       {:one-coll-one-add
                 {:rows [["a" :ready "c1"] ["b" :ready "c1"] ["c" :ready "c1"]]}
                 :two-colls-two-adds
                 {:rows [["a" :ready "c1"] ["b" :ready "c2"] ["c" :ready "c1"]
                         ["d" :no-op "c1"] ["e" :err "c1"]]}
                 :failed-batch-names-the-bad-id
                 {:rows        [["a" :ready "c1"] ["b" :ready "c1"] ["c" :ready "c1"]]
                  :fail-batch? true
                  :bad-ids     ["b"]}}
   :gen         gen-page
   :pred        :ok?
   :num-tests   200
   :mutations   [["lose-attribution"
                  (fn [{:keys [rows]}]
                    {:batch-calls {"c1" 1} :disposed #{}
                     :outcomes (into (sorted-map)
                                     (map (fn [[id]] [id :boundary/milvus-write-failed]))
                                     rows)
                     :ok? false})]
                 ["delete-before-write"
                  (fn [{:keys [rows]}]
                    {:batch-calls {} :disposed (set (map first rows))
                     :outcomes (into (sorted-map) (map (fn [[id]] [id :placed])) rows)
                     :ok? true})]]})

;; ============================================================
;; Focused
;; ============================================================

(deftest a-page-of-n-ids-into-one-collection-is-one-add
  (let [calls (atom [])
        rows  (mapv (fn [i] [(str "id" i) :ready "c1"]) (range 50))
        out   (p/commit-page (->StubWriter calls false #{})
                             (->RecordingDisposition (atom []))
                             (prepared-page rows))]
    (is (= 1 (count @calls)) "50 records, one round-trip")
    (is (= 50 (count (second (first @calls)))))
    (is (every? (comp r/ok? second) out))))

(deftest a-failed-batch-keeps-per-id-errors
  (testing "the batch fails, the fallback writes each record and names the one that failed"
    (let [out (commit-outcome {:rows        [["a" :ready "c1"] ["b" :ready "c1"] ["c" :ready "c1"]]
                               :fail-batch? true
                               :bad-ids     ["b"]})]
      (is (= {"a" :placed "b" :boundary/milvus-write-failed "c" :placed} (:outcomes out)))
      (is (= #{"a" "c"} (:disposed out)) "the source of the failed id is never deleted"))))

(deftest unwrap-placed-maps-results-onto-the-runner-shape
  (is (= {:placed? true :moved? true :id "a"}
         (relocate/unwrap-placed "a" (r/ok {:placed? true :id "a"}))))
  (is (= {:moved? false :from nil :to nil :id "a" :reason :not-found}
         (relocate/unwrap-placed "a" (r/err :collector/not-found {:id "a"}))))
  (is (= {:moved? false :error :embedder/embed-failed :id "a"}
         (relocate/unwrap-placed "a" (r/err :embedder/embed-failed {}))))
  (is (= {:moved? false :error "boom" :id "a"}
         (relocate/unwrap-placed "a" {:moved? false :error "boom" :id "a"}))))
