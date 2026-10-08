(ns hive-milvus.store.entries-read-outcome-test
  "A degraded Milvus must answer a read with an error, never with absence.

   Every collaborator is a port handed in as a function or a stub record: a
   FETCH `(fn [coll id] entry-or-nil)`, a DELETE `(fn [coll id])`, and the
   search pipeline's stub resolver/embedder/searcher. No live Milvus, no
   redefined vars."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-milvus.failure :as failure]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.search.boundary :as b]
            [hive-milvus.store.search.pipeline :as pipeline]
            [hive-milvus.store.search.target :as tgt]
            [hive-milvus.store.lookup :as lookup]))

(def ^:private timeout-msg "milvus get timed out after 5000ms")

(defn- timeout!
  "What `store.deref/deref!` throws when a Milvus RPC overruns its budget."
  []
  (throw (ex-info timeout-msg {:milvus/timeout true :op :get :timeout-ms 5000})))

(defn- stub-fetch
  "FETCH over `by-coll`: coll -> {id entry} or :down (every read throws)."
  [by-coll]
  (fn [coll id]
    (let [c (get by-coll coll)]
      (if (= :down c) (timeout!) (get c id)))))

;; =============================================================================
;; get-entry: locate-entry over a FETCH port
;; =============================================================================

(deftest an-unreachable-collection-is-not-absence-test
  (testing "the id is absent from the healthy collection, the other is down"
    (let [res (entries/locate-entry (stub-fetch {"a" {} "b" :down}) ["a" "b"] "x")]
      (is (r/err? res) "a miss with a failed collection is NOT 'not found'")
      (is (= [{:collection "b" :message timeout-msg}] (:failed res)))
      (is (:milvus/timeout (ex-data (:cause res))) "the cause travels with the error"))))

(deftest a-hit-in-a-healthy-collection-wins-test
  (let [e   {:id "x" :content "hi"}
        res (entries/locate-entry (stub-fetch {"a" :down "b" {"x" e}}) ["a" "b"] "x")]
    (is (r/ok? res))
    (is (= e (:ok res)) "the entry is returned despite a failing neighbour")))

(deftest every-collection-answering-empty-is-absence-test
  (let [res (entries/locate-entry (stub-fetch {"a" {} "b" {}}) ["a" "b"] "x")]
    (is (= (r/ok nil) res) "only a full set of answers licenses nil")))

(deftest no-collections-is-absence-test
  (is (= (r/ok nil) (entries/locate-entry (stub-fetch {}) [] "x"))))

(deftest the-first-hit-stops-the-scan-test
  (let [asked (atom [])
        fetch (fn [coll id] (swap! asked conj coll) (when (= coll "a") {:id id}))]
    (is (r/ok? (entries/locate-entry fetch ["a" "b" "c"] "x")))
    (is (= ["a"] @asked))))

(deftest a-failed-lookup-raises-a-classifiable-exception-test
  (testing "the raised exception carries the cause, so `resilient` retries it"
    (let [res (entries/locate-entry (stub-fetch {"a" :down}) ["a"] "x")
          ex  (try (entries/located-value res) nil
                   (catch clojure.lang.ExceptionInfo e e))]
      (is (some? ex) "located-value throws rather than answering nil")
      (is (= [{:collection "a" :message timeout-msg}]
             (:hive-milvus/failed-collections (ex-data ex))))
      (is (failure/transient? (failure/classify ex))
          "a timeout stays transient through the wrapper")))
  (testing "found and absent pass through as values"
    (is (= {:id "x"} (entries/located-value (r/ok {:id "x"}))))
    (is (nil? (entries/located-value (r/ok nil))))))

;; =============================================================================
;; delete-entry!: a delete that did not reach a collection did not land
;; =============================================================================

(deftest a-delete-that-fails-somewhere-is-not-success-test
  (let [del (fn [coll _id] (when (= coll "b") (timeout!)))
        res (entries/delete-everywhere del ["a" "b"] "x")]
    (is (r/err? res))
    (is (= ["b"] (mapv :collection (:failed res))))))

(deftest a-delete-that-reaches-every-collection-lands-test
  (let [seen (atom [])
        res  (entries/delete-everywhere (fn [coll id] (swap! seen conj [coll id]))
                                        ["a" "b"] "x")]
    (is (= (r/ok true) res))
    (is (= [["a" "x"] ["b" "x"]] @seen))))

;; =============================================================================
;; search-similar: a failed target travels with the results
;; =============================================================================

;; =============================================================================
;; A configured collection that was never created holds nothing: answered-absent
;; =============================================================================

(defn- missing!
  "What Milvus answers for a collection that does not exist (gRPC status 100)."
  [coll]
  (throw (ex-info (str "Milvus get failed: collection not found[collection=" coll "]")
                  {:status 100})))

(deftest a-never-created-collection-is-recognised-test
  (is (lookup/missing-collection? (ex-info "collection not found[collection=c]" {})))
  (is (lookup/missing-collection?
       (java.util.concurrent.ExecutionException.
        (ex-info "Milvus HTTP /v2/vectordb/entities/get returned code=100 message=collection not found[collection=c]" {}))))
  (is (lookup/missing-collection? (RuntimeException. "can't find collection: c")))
  (is (not (lookup/missing-collection? (ex-info timeout-msg {:milvus/timeout true}))))
  (is (not (lookup/missing-collection? (RuntimeException. "UNAVAILABLE: io exception")))))

(deftest a-never-created-collection-is-absence-not-failure-test
  (let [fetch (fn [coll id] (if (= coll "new_1024d") (missing! coll) (get {} id)))]
    (is (= (r/ok nil) (entries/locate-entry fetch ["old" "new_1024d"] "x"))
        "a miss everywhere else plus a collection that does not exist is absence"))
  (let [e     {:id "x"}
        fetch (fn [coll id] (if (= coll "new_1024d") (missing! coll) ({"x" e} id)))]
    (is (= (r/ok e) (entries/locate-entry fetch ["new_1024d" "old"] "x")))))

(deftest a-never-created-collection-takes-a-delete-test
  (let [del (fn [coll _id] (when (= coll "new_1024d") (missing! coll)))]
    (is (= (r/ok true) (entries/delete-everywhere del ["old" "new_1024d"] "x"))
        "nothing to delete there, so the delete landed")))

(deftest a-missing-collection-does-not-hide-a-real-outage-test
  (let [fetch (fn [coll _id] (case coll "new_1024d" (missing! coll) "down" (timeout!) nil))
        res   (entries/locate-entry fetch ["new_1024d" "down"] "x")]
    (is (r/err? res))
    (is (= ["down"] (mapv :collection (:failed res))))))

;; =============================================================================
;; A delete that did not land says so as a WRITE, not a read
;; =============================================================================

(deftest an-incomplete-delete-raises-a-write-message-test
  (let [res (entries/delete-everywhere (fn [_ _] (timeout!)) ["a"] "x")
        ex  (try (entries/deleted-value res) nil
                 (catch clojure.lang.ExceptionInfo e e))]
    (is (some? ex))
    (is (re-find #"delete of \"x\"" (ex-message ex)))
    (is (not (re-find #"read" (ex-message ex))) "a delete is not a read")
    (is (= [{:collection "a" :message timeout-msg}]
           (:hive-milvus/failed-collections (ex-data ex))))
    (is (failure/transient? (failure/classify ex)) "cause kept, so resilient retries it"))
  (is (true? (entries/deleted-value (r/ok true)))))

;; =============================================================================
;; update-entry!: the relocate pipeline's error says whether a retry can help
;; =============================================================================

(deftest a-pipeline-error-is-marked-transient-or-permanent-test
  (testing "a write the transport dropped is transient"
    (let [m (entries/update-failure "x" {:error   :boundary/milvus-write-failed
                                         :class   "class clojure.lang.ExceptionInfo"
                                         :message "milvus add timed out after 5000ms: UNAVAILABLE"})]
      (is (= :boundary/milvus-write-failed (:error m)))
      (is (= "x" (:id m)))
      (is (true? (:reconnecting? m)))))
  (testing "routing / embedding / schema errors are permanent"
    (is (false? (:reconnecting? (entries/update-failure "x" {:error :routing/no-target :type "bogus"}))))
    (is (false? (:reconnecting? (entries/update-failure
                                 "x" {:error   :boundary/milvus-write-failed
                                      :message "field embedding: dim mismatch"}))))))

(deftest an-incomplete-locate-is-retryable-when-its-cause-is-test
  (testing "the relocate collector's outage error re-queues when a collection timed out"
    (let [res (lookup/locate-collection (stub-fetch {"a" {} "b" :down}) ["a" "b"] "x")
          m   (entries/update-failure "x" res)]
      (is (= :milvus/read-incomplete (:error m)))
      (is (true? (:reconnecting? m)))))
  (testing "a non-transport failure stays permanent"
    (is (false? (:reconnecting? (entries/update-failure
                                 "x" {:error  :milvus/read-incomplete
                                      :id     "x"
                                      :failed [{:collection "a"
                                                :message    "field embedding: dim mismatch"}]})))))
  (testing "an absent id is never an outage"
    (is (= (r/ok nil)
           (lookup/locate-collection (stub-fetch {"a" {} "b" {}}) ["a" "b"] "x")))))

(defn- row [id d]
  {:id id :distance d :type "note" :tags "[]" :content id :content_hash ""
   :created "2026-07-12T00:00:00Z" :updated "2026-07-12T00:00:00Z"
   :duration "medium" :access_count 0})

(defn- ctx [rows]
  (pipeline/context
   {:resolver (tgt/fixed-resolver [(tgt/->target "old_1024d" :old)
                                   (tgt/->target "new-2560d" :new)])
    :embedder (b/stub-embedder {:old [0.1] :new [0.2]})
    :searcher (b/stub-vector-search rows)}))

(deftest a-failing-search-target-is-carried-as-metadata-test
  (let [res (entries/search-with (ctx {"new-2560d" [(row "a" 0.1)]}) "q" {:limit 5})]
    (is (= ["a"] (mapv :id res)) "the healthy space still answers")
    (is (= ["old_1024d"]
           (mapv :collection (:hive-milvus/failed-collections (meta res)))))
    (is (every? string? (map :message (:hive-milvus/failed-collections (meta res))))
        "same {:collection :message} shape as query-entries")))

(deftest every-search-target-down-is-not-an-honest-empty-test
  (let [res (entries/search-with (ctx {}) "q" {:limit 5})]
    (is (empty? res))
    (is (= 2 (count (:hive-milvus/failed-collections (meta res))))
        "the emptiness is marked as an artifact of the failure")))

(deftest a-clean-search-carries-no-failure-metadata-test
  (let [res (entries/search-with (ctx {"new-2560d" [(row "a" 0.1)]
                                       "old_1024d" []})
                                 "q" {:limit 5})]
    (is (= ["a"] (mapv :id res)))
    (is (nil? (:hive-milvus/failed-collections (meta res))))))
