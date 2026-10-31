(ns hive-milvus.store.batch
  "Batch protocol helpers for MilvusMemoryStore."
  (:require [hive-dsl.result :as r]
            [hive-milvus.resilience.budget :as budget]
            [hive-milvus.store.deref :as d]
            [hive-milvus.resilience.retry :refer [resilient]]
            [hive-milvus.store.lookup :as lookup]
            [hive-milvus.store.schema :as schema]
            [malli.core :as m]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]))

(def page-size
  "Ids per Milvus get. Measured 2026-10-12: 50 ids answered in 1.1 s, while
   200 or more passed the 5 s deref budget and came back as nothing."
  50)

(def get-row-cost-ms
  "Allowance per id of a get that returns content (measured about 22 ms)."
  25)

(defn page-deadline-ms
  "Deref bound for a get of `n` ids: the base deadline plus `get-row-cost-ms`
   per id, capped by `budget/read-deadline-cap-ms`."
  [n]
  (budget/read-deadline-ms (d/timeout-ms) n get-row-cost-ms))

(defn gather
  "Fold a batch get over `colls`, `ids` read in pages of `page-size`, through
   FETCH (`(fn [coll ids] rows)`). First hit per :id wins, in collection order
   (the default 768-d collection comes first, so legacy entries take
   precedence over orphaned post-migration duplicates).

   A collection that does not exist (`lookup/missing-collection?`) answered:
   it holds nothing. Any other failure of a page is recorded, never read as
   absence. Returns the raw rows when every page answered, else
   `(r/err :milvus/read-incomplete {:failed [{:collection :message :ids n}] :partial rows})`.
   Pure apart from FETCH."
  [fetch colls ids]
  (let [pages (partition-all page-size ids)
        {:keys [by-id order failed]}
        (reduce (fn [acc [coll page]]
                  (try
                    ;; Dedupe by :id — first hit wins (default 768-d collection
                    ;; comes first, so legacy entries take precedence over any
                    ;; orphaned post-migration duplicates).
                    (reduce (fn [a row]
                              (if (contains? (:by-id a) (:id row))
                                a
                                (-> a
                                    (assoc-in [:by-id (:id row)] row)
                                    (update :order conj (:id row)))))
                            acc
                            (fetch coll (vec page)))
                    (catch Exception e
                      (if (lookup/missing-collection? e)
                        acc
                        (update acc :failed conj
                                {:collection coll
                                 :message    (or (ex-message e) (str (class e)))
                                 :ids        (count page)})))))
                {:by-id {} :order [] :failed []}
                (for [coll colls page pages] [coll page]))
        rows (mapv by-id order)]
    (if (seq failed)
      (r/err :milvus/read-incomplete {:failed failed :partial rows})
      rows)))

(m/=> gather [:=> [:cat fn? [:sequential :string] [:sequential :any]] [:or [:vector :map] :map]])

(defn get-entries
  "IMemoryStoreBatch: the entries under `ids` that exist, in no promised
   order; nil when no non-nil id is asked for. Ids are read in pages of
   `page-size`, each under a deadline scaled to the page (`page-deadline-ms`),
   so a large ask is not one get that times out. A page that a collection
   could not answer makes the result an error Result
   (`:milvus/read-incomplete`, see `gather`), never a short vector that reads
   as 'these ids do not exist'."
  [config-atom ids]
  (let [ids (vec (distinct (remove nil? ids)))]
    (when (seq ids)
      (resilient config-atom
        (let [result (gather (fn [coll-name page]
                               (d/deref! :get (milvus/get coll-name page
                                                :consistency-level :bounded
                                                :include schema/default-read-fields)
                                         (page-deadline-ms (count page))))
                             (lookup/known-collections config-atom)
                             ids)]
          (if (r/err? result)
            (do (log/warn "milvus get-entries incomplete:" (count (:failed result))
                          "page(s) unanswered," (count (:partial result)) "rows read")
                (update result :partial #(mapv schema/record->entry %)))
            (mapv schema/record->entry result)))))))
