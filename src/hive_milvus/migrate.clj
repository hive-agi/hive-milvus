(ns hive-milvus.migrate
  "Chroma SQLite → Milvus migration.

   Railway-oriented pipeline: resolve-config → read-source → resolve-cursor
   → connect-target → migrate-batches!

   Each step returns Result (ok/err). Cursor persists after each batch
   for safe interrupt/resume."
  (:require [hive-di.core :refer [defconfig env literal]]
            [hive-dsl.result :as r]
            [hive-milvus.batch :as batch]
            [hive-milvus.collections :as collections]
            [hive-milvus.cursor :as cursor]
            [hive-milvus.dimensions :as dimensions]
            [hive-milvus.embed :as embed]
            [hive-milvus.record :as record]
            [hive-milvus.sqlite :as sqlite]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Config
;; =============================================================================

(defconfig MigrateConfig
  :sqlite-path       (env "CHROMA_SQLITE_PATH"
                          :default (str (System/getProperty "user.home")
                                        "/backups/chroma/chroma-20260404T202814.sqlite3")
                          :type :string
                          :doc "Path to Chroma SQLite backup file")
  :chroma-collection (literal "hive-mcp-memory"
                              :doc "Source Chroma collection name")
  :milvus-host       (env "MILVUS_HOST" :default "localhost" :type :string)
  :milvus-port       (env "MILVUS_PORT" :default 19530 :type :int)
  :milvus-collection (literal "hive_mcp_memory"
                              :doc "Target Milvus collection name")
  :batch-size        (env "MIGRATE_BATCH_SIZE" :default 50 :type :int)
  :skip-empty        (literal true :doc "Skip entries with blank content")
  :ollama-host       (env "OLLAMA_HOST" :default "localhost:11434" :type :string)
  :embed-model       (env "EMBED_MODEL" :default "nomic-embed-text" :type :string)
  :cursor-path       (env "MIGRATE_CURSOR_PATH"
                          :default (str (System/getProperty "user.home")
                                        "/backups/chroma/migration-cursor.edn")
                          :type :string
                          :doc "Path to cursor file for resume support"))

;; =============================================================================
;; Pipeline Steps (each returns Result)
;; =============================================================================

(defn resolve-config
  "Step 1: Resolve and validate migration config."
  [overrides]
  (let [result (resolve-MigrateConfig overrides)]
    (if (r/err? result)
      (r/err :config/invalid {:message "Migration config resolution failed"})
      (r/ok (merge (:ok result)
                   (select-keys overrides [:dry-run? :reset-cursor? :limit]))))))

(defn read-source
  "Step 2: Read and filter entries from Chroma SQLite."
  [{:keys [sqlite-path chroma-collection skip-empty limit] :as ctx}]
  (r/try-effect* :source/read-failed
    (let [all     (sqlite/read-all-entries sqlite-path chroma-collection)
          entries (cond->> all
                    skip-empty (filter #(seq (:content %)))
                    limit      (take limit))]
      (log/info "Read" (count entries) "entries from" sqlite-path
                {:total-in-db (count all)})
      (assoc ctx :entries (vec entries) :total (count entries)))))

(defn resolve-resume
  "Step 3: Determine resume point from cursor. Pure logic + cursor IO."
  [{:keys [cursor-path batch-size total reset-cursor? entries] :as ctx}]
  (r/try-effect* :cursor/resolve-failed
    (let [existing    (when-not reset-cursor? (cursor/read-cursor cursor-path))
          resumable?  (and existing
                           (= (:batch-size existing) batch-size)
                           (not= (:status existing) :completed))
          completed?  (and existing (= (:status existing) :completed))]

      (when (and completed? (not reset-cursor?))
        (throw (ex-info "Migration already completed. Use :reset-cursor? true to re-run."
                        {:cursor existing})))

      (if resumable?
        (let [skip (* (:completed-batches existing) batch-size)]
          (log/info "RESUMING from batch" (inc (:completed-batches existing))
                    "— skipping" skip "entries (" (:inserted existing) "inserted so far)")
          (assoc ctx
                 :cursor existing
                 :batch-base (:completed-batches existing)
                 :remaining (vec (drop skip entries))))
        (do (when (and existing reset-cursor?)
              (log/info "Cursor reset — starting fresh"))
            (assoc ctx
                   :cursor (cursor/fresh-cursor total batch-size)
                   :batch-base 0
                   :remaining entries))))))

(defn connect-target
  "Step 4: Connect to Milvus and ensure collection exists with correct dimension."
  [{:keys [milvus-host milvus-port milvus-collection sqlite-path chroma-collection] :as ctx}]
  (r/try-effect* :target/connect-failed
    (require '[milvus-clj.api :as milvus]
             '[milvus-clj.schema :as milvus-schema]
             '[milvus-clj.index :as milvus-index])
    ((requiring-resolve 'milvus-clj.api/connect!)
     {:host milvus-host :port milvus-port})
    (let [dim (dimensions/detect-dimension sqlite-path chroma-collection)]
      (when-not @((requiring-resolve 'milvus-clj.api/has-collection) milvus-collection)
        (log/info "Creating collection" milvus-collection "with dimension" dim)
        @((requiring-resolve 'milvus-clj.api/create-collection) milvus-collection
          {:schema ((requiring-resolve 'milvus-clj.schema/with-dimension) dim)
           :index  @(requiring-resolve 'milvus-clj.index/default-memory-index)
           :description (str "Migrated from Chroma: " chroma-collection)}))
      (assoc ctx :dimension dim))))

;; =============================================================================
;; Public API
;; =============================================================================

(defn run!
  "Run the Chroma → Milvus migration pipeline for a single collection.

   Resumes automatically from cursor. Upsert makes partial-batch replay safe.

   Overrides:
     :dry-run?       — report only, no writes
     :limit          — cap entries to migrate
     :reset-cursor?  — discard cursor, start fresh"
  [overrides]
  (let [result (-> (resolve-config overrides)
                   (r/bind read-source)
                   (r/bind resolve-resume)
                   (r/bind connect-target)
                   (r/bind batch/migrate-batches!))]
    (if (r/err? result)
      (do (log/error "Migration failed:" result)
          result)
      (:ok result))))

(defn run-all!
  "Migrate ALL non-test Chroma collections to Milvus.

   For each collection:
   1. Creates a Milvus collection with matching dimension
   2. Uses per-collection cursor for safe resume
   3. Skips test collections (patterns: *-test, test-*, *-e2e-*, *-test-*)

   Overrides: same as run! plus
     :include-empty?  — include collections with 0 entries (default: false)
     :collections     — explicit vec of collection names (overrides auto-discovery)"
  [overrides]
  (let [config-result (resolve-config overrides)]
    (if (r/err? config-result)
      (do (log/error "Config resolution failed:" config-result)
          config-result)
      (let [{:keys [sqlite-path cursor-path]} (:ok config-result)
            all-cols   (or (:collections overrides)
                           (->> (sqlite/list-collections sqlite-path)
                                (remove #(collections/test-collection? (:name %)))
                                (remove #(and (not (:include-empty? overrides))
                                              (zero? (:entry-count %))))
                                (mapv :name)))
            results    (atom [])]
        (log/info "=== Multi-collection migration ===" {:collections (count all-cols)})
        (doseq [coll-name all-cols]
          (let [milvus-name (collections/collection->milvus-name coll-name)
                coll-cursor (cursor/cursor-path-for cursor-path coll-name)
                coll-overrides (merge overrides
                                      {:chroma-collection coll-name
                                       :milvus-collection milvus-name
                                       :cursor-path       coll-cursor})]
            (log/info "--- Starting collection:" coll-name "→" milvus-name "---")
            (let [dim (dimensions/detect-dimension sqlite-path coll-name)
                  result (if (nil? dim)
                           (do (log/info "Skipping" coll-name "(noop embedding, no vectors)")
                               {:status :skipped :reason :noop-embedding})
                           (run! coll-overrides))]
              (swap! results conj {:collection coll-name
                                   :milvus     milvus-name
                                   :result     (if (r/err? result) :failed :ok)
                                   :details    result}))))
        (let [summary {:total      (count all-cols)
                       :succeeded  (count (filter #(= :ok (:result %)) @results))
                       :failed     (count (filter #(= :failed (:result %)) @results))
                       :results    @results}]
          (log/info "=== Multi-collection migration complete ===" (dissoc summary :results))
          summary)))))

;; =============================================================================
;; Incremental Sync (delta-only migration)
;; =============================================================================

(defn- milvus-existing-ids
  "Query Milvus for all entry IDs in a collection via cursor-paginated scalar query.
   Uses lexicographic ID ordering: each page filters id > last-seen-id.
   Returns a set of ID strings."
  [milvus-collection]
  (let [query-fn (requiring-resolve 'milvus-clj.api/query-scalar)
        page-size 5000]
    (loop [cursor "" all-ids #{} page 0]
      (let [batch (try
                    @(query-fn milvus-collection
                               {:filter (str "id > \"" cursor "\"")
                                :output-fields ["id"]
                                :limit page-size})
                    (catch Exception e
                      (log/error "query-scalar failed at page" page "cursor" cursor ":" (.getMessage e))
                      []))
            ids (into all-ids (map :id) batch)]
        (log/debug "Page" page ":" (count batch) "IDs (total:" (count ids) ")")
        (if (< (count batch) page-size)
          (do (log/info "Fetched" (count ids) "existing IDs from" milvus-collection
                        "in" (inc page) "pages")
              ids)
          (let [max-id (reduce (fn [a b] (if (pos? (compare b a)) b a))
                               (map :id batch))]
            (recur max-id ids (inc page))))))))

(defn sync!
  "Incremental sync: migrate only entries missing from Milvus.

   Reads all entries from Chroma SQLite, queries Milvus for existing IDs,
   and only embeds + upserts the delta. Much faster than full re-run.

   Overrides: same as run! plus
     :sqlite-path     — path to fresh Chroma SQLite backup
     :collections     — explicit vec of collection names"
  [overrides]
  (let [config-result (resolve-config overrides)]
    (if (r/err? config-result)
      (do (log/error "Sync config failed:" config-result)
          config-result)
      (let [{:keys [sqlite-path ollama-host embed-model] :as config} (:ok config-result)
            colls (or (:collections overrides) ["hive-mcp-memory"])
            results (atom [])]
        (doseq [coll-name colls]
          (let [milvus-name (collections/collection->milvus-name coll-name)
                dim (dimensions/detect-dimension sqlite-path coll-name)]
            (if (nil? dim)
              (do (log/info "Skipping" coll-name "(noop embedding)")
                  (swap! results conj {:collection coll-name :status :skipped}))
              (let [sync-result
                    (r/try-effect* :sync/failed
                      (log/info "Syncing" coll-name "→" milvus-name)
                      (let [connect-fn (requiring-resolve 'milvus-clj.api/connect!)
                            _           (connect-fn {:host (:milvus-host config)
                                                     :port (:milvus-port config)})
                            all-entries  (sqlite/read-all-entries sqlite-path coll-name)
                            existing-ids (milvus-existing-ids milvus-name)
                            new-entries  (vec (remove #(contains? existing-ids (:id %)) all-entries))
                            _            (log/info "Delta:" (count new-entries) "new of" (count all-entries) "total")]
                        (if (empty? new-entries)
                          (do (log/info "No new entries for" coll-name)
                              (swap! results conj {:collection coll-name :status :up-to-date
                                                   :total (count all-entries)}))
                          (let [batches        (partition-all (:batch-size config) new-entries)
                                total-inserted (atom 0)
                                total-skipped  (atom 0)]
                            (doseq [[i b] (map-indexed vector batches)]
                              (log/info "Batch" (inc i) "/" (count batches)
                                        "(" (count b) "entries)")
                              (let [r (batch/process-batch! (vec b) ollama-host embed-model
                                                            milvus-name dim)]
                                (swap! total-inserted + (:inserted r 0))
                                (swap! total-skipped + (:skipped r 0))))
                            (swap! results conj {:collection coll-name :status :synced
                                                 :new (count new-entries)
                                                 :inserted @total-inserted
                                                 :skipped @total-skipped})))))]
                (when (r/err? sync-result)
                  (log/error "Sync failed for" coll-name ":" sync-result)
                  (swap! results conj {:collection coll-name :status :error
                                       :error (str sync-result)}))))))
        (let [summary {:collections (count colls) :results @results}]
          (log/info "Sync complete:" summary)
          summary)))))

;; =============================================================================
;; Backward-compat re-exports
;; =============================================================================

(def get-connection sqlite/get-connection)
(def read-all-entries sqlite/read-all-entries)
(def list-collections sqlite/list-collections)
(def rows->entries sqlite/rows->entries)
(def embed-text embed/embed-text)
(def entry->record record/entry->record)
(def read-cursor cursor/read-cursor)
(def embed-entry batch/embed-entry)
(def process-batch! batch/process-batch!)
(def migrate-batches! batch/migrate-batches!)
