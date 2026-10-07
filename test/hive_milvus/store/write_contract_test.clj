(ns hive-milvus.store.write-contract-test
  "MilvusMemoryStore's write verbs held to hive-spi.memory.contract.

   `update-result` (pure) maps a relocate-update Result onto UpdateResult,
   covered by a trifecta. The write cases of hive-spi's conformance suite
   then run against a real MilvusMemoryStore whose milvus-clj client port is
   an in-memory fake (`hive-milvus.fake-milvus`): no live Milvus, no
   redefined vars."
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.result :as r]
            [hive-milvus.fake-milvus :as fake]
            [hive-milvus.store :as store]
            [hive-milvus.store.entries :as entries]
            [hive-spi.memory.conformance :as conformance]
            [hive-spi.memory.contract :as contract]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-spi.memory.ports :as ports]))

;; =============================================================================
;; update-result: relocate-update Result -> UpdateResult
;; =============================================================================

(defn- answer
  "Harness: run `update-result` on [id res] and project what the contract
   cares about - the outcome class and whether a landed entry kept `id`."
  [[id res]]
  (let [v (entries/update-result id res)]
    {:outcome (contract/outcome :update-entry! v)
     :id-kept (when (map? v) (= id (:id v)))}))

(def ^:private ok-merged      ["e1" (r/ok {:id "e1" :content "after"})])
(def ^:private ok-foreign-id  ["e1" (r/ok {:id "other" :content "after"})])
(def ^:private ok-no-id       ["e1" (r/ok {:content "after"})])
(def ^:private not-found      ["e1" (r/err :collector/not-found {:id "e1"})])
(def ^:private write-failed   ["e1" (r/err :boundary/milvus-write-failed
                                           {:message "UNAVAILABLE: io exception"})])
(def ^:private embed-failed   ["e1" (r/err :embedder/embed-failed {:message "no provider"})])

(def ^:private gen-res
  (gen/one-of
   [(gen/fmap (fn [m] (r/ok m))
              (gen/map (gen/elements [:id :content :tags :type]) gen/string-alphanumeric))
    (gen/return (r/err :collector/not-found {}))
    (gen/fmap (fn [k] (r/err k {:message "boom"}))
              (gen/elements [:boundary/milvus-write-failed :embedder/embed-failed
                             :routing/no-target :collector/entry-vanished]))]))

(def ^:private gen-input
  (gen/tuple (gen/not-empty gen/string-alphanumeric) gen-res))

(defn- honours-contract?
  [{:keys [outcome id-kept]}]
  (case outcome
    :landed (true? id-kept)
    (:absent :failed) true
    false))

(deftrifecta update-result-honours-the-write-contract
  answer
  {:golden-path "test/golden/milvus/trifecta-update-result.edn"
   :cases       {:ok-merged     ok-merged
                 :ok-foreign-id ok-foreign-id
                 :ok-no-id      ok-no-id
                 :not-found     not-found
                 :write-failed  write-failed
                 :embed-failed  embed-failed}
   :gen         gen-input
   :pred        honours-contract?
   :num-tests   200
   :mutations   [["raw-ok-keeps-foreign-id"
                  ;; the pre-fix shape: the pipeline's map passed through as is
                  (fn [[id res]]
                    (let [v (cond (r/ok? res) (:ok res)
                                  (= :collector/not-found (:error res)) nil
                                  :else {:error (:error res) :id id})]
                      {:outcome (contract/outcome :update-entry! v)
                       :id-kept (when (map? v) (= id (:id v)))}))]
                 ["absent-as-failure"
                  (fn [[id res]]
                    (let [v (if (r/ok? res) (assoc (:ok res) :id id) {:error :absent})]
                      {:outcome (contract/outcome :update-entry! v)
                       :id-kept (when (map? v) (= id (:id v)))}))]
                 ["failure-as-nil"
                  (fn [[id res]]
                    (let [v (when (r/ok? res) (assoc (:ok res) :id id))]
                      {:outcome (contract/outcome :update-entry! v)
                       :id-kept (when (map? v) (= id (:id v)))}))]]})

;; =============================================================================
;; Conformance write cases over the fake client port
;; =============================================================================

(def ^:private restore (atom nil))

(use-fixtures :once
  (fn [f]
    (reset! restore (fake/install!))
    (try (f) (finally (@restore)))))

(defn- fake-store
  "A MilvusMemoryStore over a fresh in-memory client."
  []
  (@restore)
  (reset! restore (fake/install!))
  (store/create-store {:collection-name fake/collection}))

(def ^:private write-cases
  [:add-returns-id :add-generates-id :update-merges :write-returns-honour-contract
   :update-unknown-is-absent :delete-then-get-is-nil :delete-unknown-is-true
   :delete-unknown-does-not-throw :add-delete-count-invariant :metadata-write])

(deftest milvus-honours-the-write-contract
  (doseq [id write-cases]
    (is (= :ran (conformance/run-case fake-store {} id)) (str id))))

(deftest update-cannot-rename-an-entry
  (let [s  (fake-store)
        e  (conformance/make-entry {:content "before"})
        id (ports/add-entry! s e)
        r  (ports/update-entry! s id {:id "hijack" :content "after"})]
    (is (= id (:id r)))
    (is (= "after" (:content (ports/get-entry s id))))
    (is (nil? (ports/get-entry s "hijack"))
        "the :id in the updates did not mint a second row")))
