(ns hive-milvus.batch
  "Batch processing: embed entries + upsert to Milvus + persist cursor."
  (:require [hive-dsl.result :as r]
            [hive-milvus.cursor :as cursor]
            [hive-milvus.embed :as embed]
            [hive-milvus.record :as record]
            [taoensso.timbre :as log]))

(defn embed-entry
  "Embed a single entry. Empty content gets a zero vector (preserves ID for KG).
   Returns {:record ... :skipped? false} or {:skipped? true :id ...}."
  [entry ollama-host model dimension]
  (let [text (or (:content entry) "")]
    (if (empty? text)
      {:skipped? false :record (record/entry->record entry (vec (repeat dimension 0.0)))}
      (let [result (embed/embed-text text ollama-host model)]
        (if (r/ok? result)
          {:skipped? false :record (record/entry->record entry (:ok result))}
          (do (log/warn "Failed to embed" {:id (:id entry) :error (:message result)})
              {:skipped? true :id (:id entry)}))))))

(defn process-batch!
  "Embed entries and upsert to Milvus. Returns {:inserted N :skipped N}."
  [entries ollama-host model milvus-collection dimension]
  (let [results  (mapv #(embed-entry % ollama-host model dimension) entries)
        records  (into [] (comp (remove :skipped?) (map :record)) results)
        skipped  (count (filter :skipped? results))]
    (when (seq records)
      @((requiring-resolve 'milvus-clj.api/add) milvus-collection records :upsert? true))
    {:inserted (count records) :skipped skipped}))

(defn migrate-batches!
  "Step 5: Process all remaining batches with cursor persistence."
  [{:keys [remaining batch-size batch-base cursor cursor-path
           ollama-host embed-model total milvus-collection dimension] :as ctx}]
  (if (:dry-run? ctx)
    (do (log/info "DRY RUN — " total "entries," (count remaining) "remaining")
        (doseq [e (take 3 remaining)]
          (log/info {:id (:id e) :type (:type e) :content-len (count (:content e))}))
        (r/ok {:status :dry-run :total total :cursor cursor}))

    (r/try-effect* :migrate/batch-failed
      (let [batches    (partition-all batch-size remaining)
            start-ms   (System/currentTimeMillis)
            inserted   (atom (or (:inserted cursor) 0))
            skipped    (atom (or (:skipped cursor) 0))
            skip-count (* batch-base batch-size)]

        (log/info "Inserting" (count remaining) "entries in batches of" batch-size)

        (doseq [[i batch] (map-indexed vector batches)]
          (let [batch-idx   (+ batch-base i)
                batch-start (System/currentTimeMillis)
                result      (process-batch! (vec batch) ollama-host embed-model milvus-collection dimension)
                elapsed     (- (System/currentTimeMillis) batch-start)
                done        (+ skip-count (min (* (inc i) batch-size) (count remaining)))]

            (swap! inserted + (:inserted result))
            (swap! skipped + (:skipped result))

            (cursor/write-cursor! cursor-path
                                  (assoc cursor
                                         :completed-batches (inc batch-idx)
                                         :inserted @inserted
                                         :skipped @skipped
                                         :last-batch-at (cursor/now-iso)))

            (log/info (format "Batch %d/%d (%d/%d) %dms [%d inserted, %d skipped]"
                              (inc batch-idx) (+ batch-base (count batches))
                              done total elapsed @inserted @skipped))))

        (let [total-ms (- (System/currentTimeMillis) start-ms)
              final    {:status :completed
                        :completed-batches (+ batch-base (count batches))
                        :inserted @inserted :skipped @skipped
                        :completed-at (cursor/now-iso) :last-batch-at (cursor/now-iso)}]
          (cursor/write-cursor! cursor-path (merge cursor final))
          (log/info "=== Migration complete ==="
                    {:total total :inserted @inserted :skipped @skipped
                     :elapsed-ms total-ms
                     :rate (format "%.1f entries/sec"
                                   (/ (* (- @inserted (or (:inserted cursor) 0)) 1000.0)
                                      (max total-ms 1)))})
          final)))))
