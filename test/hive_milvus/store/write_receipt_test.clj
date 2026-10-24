(ns hive-milvus.store.write-receipt-test
  "Receipts use the exact persisted normalization, without a read-back."
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [hive-milvus.fake-milvus :as fake]
            [hive-milvus.store :as store]
            [hive-milvus.store.write :as write]
            [hive-milvus.store.guard :as guard]
            [hive-milvus.test-isolation :as iso]
            [hive-spi.memory.ports :as ports]))

(use-fixtures :each
  (fn [f]
    (let [restore (fake/install!)]
      (try (iso/isolate-resilience f) (finally (restore))))))

(deftest receipt-equals-read-back
  (doseq [entry [{:content "plain"}
                 {:id "provided" :type "note" :duration "long"
                  :created "2026-01-01" :updated "2026-01-02"
                  :content {:nested [1 true nil]} :tags ["one" "two"]
                  :project-id "scratch" :content-hash "hash"
                  :access-count 3 :helpful-count 2 :unhelpful-count 1}
                 {:id "json-string" :content "{\"a\":1}" :expires ""}
                 {:id "defaults" :content nil :tags [] :project-id ""}]]
    (let [s (store/create-store {:collection-name fake/collection})
          receipt (write/add-entry-returning! (:config-atom s) entry)
          fetched (ports/get-entry s (:id receipt))]
      (is (string? (:id receipt)))
      (is (= fetched (:entry receipt)))
      (is (= (:id receipt) (:id fetched)))
      (is (not (contains? (:entry receipt) :embedding)))
      (when (:created entry) (is (= (:created entry) (:created fetched))))
      (when (:updated entry) (is (= (:updated entry) (:updated fetched)))))))

(deftest ordinary-add-still-returns-id
  (let [s (store/create-store {:collection-name fake/collection})]
    (is (= "legacy" (ports/add-entry! s {:id "legacy" :content "unchanged"})))
    (is (= "unchanged" (:content (ports/get-entry s "legacy"))))))

(deftest refusal-is-not-a-receipt
  (let [s (store/create-store {:collection-name fake/collection})
        entry {:id "oversize" :content (apply str (repeat (inc (:content guard/varchar-limits)) "x"))}
        res (write/add-entry-returning! (:config-atom s) entry)]
    (is (= :milvus/payload-too-large (:error res)))
    (is (false? (:success? res)))
    (is (not (contains? res :entry)))
    (is (nil? (ports/get-entry s "oversize")))))

(deftest optional-protocol-is-wired-when-present
  (when-let [protocol-var (try
                           (requiring-resolve 'hive-spi.memory.receipt/IMemoryStoreAddReceipt)
                           (catch java.io.FileNotFoundException _old-spi nil))]
    (let [s (store/create-store {:collection-name fake/collection})
          add! (requiring-resolve 'hive-spi.memory.receipt/-add-entry-returning!)
          receipt (add! s {:id "through-port" :content "receipt"})]
      (is (satisfies? @protocol-var s))
      (is (= (:entry receipt) (ports/get-entry s (:id receipt)))))))