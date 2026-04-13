(ns hive-milvus.migrate
  "Chroma SQLite → Milvus migration.

   Railway-oriented pipeline: resolve-config → read-source → resolve-cursor
   → connect-target → migrate-batches!

   Each step returns Result (ok/err). Cursor persists after each batch
   for safe interrupt/resume."
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [hive-di.core :refer [defconfig env literal]]
            [hive-dsl.result :as r]
            [taoensso.timbre :as log])
  (:import [java.sql DriverManager]
           [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers]))

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
;; Source: SQLite Reader
;; =============================================================================

(defn- get-connection [sqlite-path]
  (Class/forName "org.sqlite.JDBC")
  (DriverManager/getConnection (str "jdbc:sqlite:" sqlite-path)))

(defn- get-collection-segment-id [conn collection-name]
  (with-open [stmt (.prepareStatement conn
                     "SELECT s.id FROM segments s
                      JOIN collections c ON s.collection = c.id
                      WHERE c.name = ? AND s.scope = 'METADATA'")]
    (.setString stmt 1 collection-name)
    (with-open [rs (.executeQuery stmt)]
      (when (.next rs)
        (.getString rs "id")))))

(defn- metadata->entry [embedding-id metadata]
  (let [get-str  #(get metadata % "")
        get-int  #(let [v (get metadata %)]
                    (cond (integer? v) v
                          (string? v) (try (Long/parseLong v) (catch Exception _ 0))
                          :else 0))
        tags-str (get-str "tags")]
    {:id              embedding-id
     :type            (keyword (or (get-str "type") "note"))
     :content         (get-str "content")
     :tags            (if (str/blank? tags-str)
                        []
                        (mapv str/trim (str/split tags-str #",")))
     :content-hash    (get-str "content-hash")
     :created         (get-str "created")
     :updated         (get-str "updated")
     :duration        (keyword (or (get-str "duration") "medium"))
     :expires         (let [e (get-str "expires")] (when-not (str/blank? e) e))
     :access-count    (get-int "access-count")
     :helpful-count   (get-int "helpful-count")
     :unhelpful-count (get-int "unhelpful-count")
     :project-id      (get-str "project-id")
     :staleness-alpha (get-int "staleness-alpha")
     :staleness-beta  (get-int "staleness-beta")
     :staleness-depth (get-int "staleness-depth")}))

(defn- rs->rows
  "Materialize a JDBC ResultSet of embedding_metadata into a vec of
   {:embedding-id :key :value} maps. Impure (walks rs)."
  [rs]
  (loop [acc (transient [])]
    (if (.next rs)
      (let [eid (.getString rs "embedding_id")
            k   (.getString rs "key")
            v   (or (.getString rs "string_value")
                    (let [iv (.getInt rs "int_value")]
                      (when-not (.wasNull rs) iv))
                    (let [fv (.getDouble rs "float_value")]
                      (when-not (.wasNull rs) fv)))]
        (recur (conj! acc {:embedding-id eid :key k :value v})))
      (persistent! acc))))

(defn rows->entries
  "Pure: group consecutive rows by :embedding-id and emit entries.
   Rows must be pre-sorted by :embedding-id (the SQL ORDER BY guarantees this).
   Each output entry is `(metadata->entry id merged-metadata)`."
  [rows]
  (let [seed     (fn [{:keys [embedding-id key value]}]
                   {:id embedding-id :meta {key value}})
        absorb   (fn [open {:keys [key value]}]
                   (update open :meta assoc key value))
        flush    (fn [entries open]
                   (conj entries (metadata->entry (:id open) (:meta open))))
        finalize (fn [{:keys [entries open]}]
                   (if open (flush entries open) entries))
        step     (fn [{:keys [entries open] :as acc} row]
                   (cond
                     (nil? open)                        (assoc acc :open (seed row))
                     (= (:embedding-id row) (:id open)) (assoc acc :open (absorb open row))
                     :else                              {:entries (flush entries open)
                                                         :open    (seed row)}))]
    (-> (reduce step {:entries [] :open nil} rows)
        finalize)))

(defn read-all-entries
  "Read all entries from a Chroma SQLite backup. Deterministic order (ORDER BY id)."
  [sqlite-path collection-name]
  (with-open [conn (get-connection sqlite-path)]
    (let [seg-id (get-collection-segment-id conn collection-name)]
      (when-not seg-id
        (throw (ex-info (str "Collection not found: " collection-name)
                        {:collection collection-name})))
      (with-open [stmt (.prepareStatement conn
                         "SELECT e.embedding_id, em.key, em.string_value, em.int_value, em.float_value
                          FROM embeddings e
                          JOIN embedding_metadata em ON em.id = e.id
                          WHERE e.segment_id = ?
                          ORDER BY e.embedding_id")]
        (.setString stmt 1 seg-id)
        (with-open [rs (.executeQuery stmt)]
          (rows->entries (rs->rows rs)))))))

;; =============================================================================
;; Embedding: Ollama Client
;; =============================================================================

(def ^:private http-client
  (delay (-> (HttpClient/newBuilder)
             (.connectTimeout (java.time.Duration/ofSeconds 30))
             (.build))))

(defn embed-text
  "Get embedding vector from Ollama. Returns Result."
  [text ollama-host model]
  (r/try-effect* :embed/failed
    (let [body    (json/write-str {:model model :prompt (subs text 0 (min 8000 (count text)))})
          request (-> (HttpRequest/newBuilder)
                      (.uri (URI/create (str "http://" ollama-host "/api/embeddings")))
                      (.header "Content-Type" "application/json")
                      (.POST (HttpRequest$BodyPublishers/ofString body))
                      (.timeout (java.time.Duration/ofSeconds 60))
                      (.build))
          resp    (.send @http-client request (HttpResponse$BodyHandlers/ofString))
          parsed  (json/read-str (.body resp) :key-fn keyword)]
      (:embedding parsed))))

;; =============================================================================
;; Record Mapping (pure)
;; =============================================================================

(defn- tags->json [tags]
  (if (sequential? tags) (json/write-str tags) "[]"))

(defn entry->record
  "Transform a memory entry + embedding into a Milvus insert record. Pure."
  [entry embedding]
  {:id              (or (:id entry) (str (random-uuid)))
   :embedding       embedding
   :document        (or (:content entry) "")
   :type            (or (some-> (:type entry) name) "note")
   :tags            (tags->json (:tags entry))
   :content         (or (:content entry) "")
   :content_hash    (or (:content-hash entry) "")
   :created         (or (:created entry) "")
   :updated         (or (:updated entry) "")
   :duration        (or (some-> (:duration entry) name) "medium")
   :expires         (or (:expires entry) "")
   :access_count    (long (or (:access-count entry) 0))
   :helpful_count   (long (or (:helpful-count entry) 0))
   :unhelpful_count (long (or (:unhelpful-count entry) 0))
   :project_id      (or (:project-id entry) "")})

;; =============================================================================
;; Cursor: Persistent Resume State
;; =============================================================================

(defn- now-iso []
  (str (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))))

(defn read-cursor [cursor-path]
  (when (.exists (io/file cursor-path))
    (try (edn/read-string (slurp cursor-path))
         (catch Exception e
           (log/warn "Corrupt cursor, starting fresh:" (.getMessage e))
           nil))))

(defn- write-cursor! [cursor-path cursor]
  (let [tmp (str cursor-path ".tmp")]
    (spit tmp (pr-str cursor))
    (.renameTo (io/file tmp) (io/file cursor-path))))

(defn- fresh-cursor [total batch-size]
  {:status :running, :completed-batches 0, :batch-size batch-size,
   :total-entries total, :inserted 0, :skipped 0,
   :started-at (now-iso), :last-batch-at nil})

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
    (let [all     (read-all-entries sqlite-path chroma-collection)
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
    (let [existing    (when-not reset-cursor? (read-cursor cursor-path))
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
                   :cursor (fresh-cursor total batch-size)
                   :batch-base 0
                   :remaining entries))))))

(def ^:private model->dimension
  "Known embedding model → vector dimension mapping."
  {"chroma"                   384   ;; Chroma default: all-MiniLM-L6-v2
   "openrouter"               4096  ;; qwen/qwen3-embedding-8b
   "qwen/qwen3-embedding-8b"  4096
   "ollama"                   768   ;; nomic-embed-text
   "nomic-embed-text"         768
   "noop"                     nil}) ;; no vectors, skip

(def ^:private collection->known-dimension
  "Collections with known dimensions not in metadata."
  {"hive-ingest" 4096}) ;; default ingestor collection uses openrouter/qwen3

(defn- detect-dimension
  "Detect embedding dimension for a Chroma collection.
   Checks: explicit 'dimension' metadata → 'embedding-model' metadata → default 384."
  [sqlite-path collection-name]
  (r/rescue 384
    (with-open [conn (get-connection sqlite-path)]
      ;; Try explicit dimension first
      (with-open [stmt (.prepareStatement conn
                         "SELECT cm.int_value FROM collection_metadata cm
                          JOIN collections c ON c.id = cm.collection_id
                          WHERE c.name = ? AND cm.key = 'dimension'")]
        (.setString stmt 1 collection-name)
        (with-open [rs (.executeQuery stmt)]
          (if (and (.next rs) (pos? (.getInt rs "int_value")))
            (.getInt rs "int_value")
            ;; Fall back to embedding-model → dimension lookup
            (with-open [stmt2 (.prepareStatement conn
                                "SELECT cm.str_value FROM collection_metadata cm
                                 JOIN collections c ON c.id = cm.collection_id
                                 WHERE c.name = ? AND cm.key = 'embedding-model'")]
              (.setString stmt2 1 collection-name)
              (with-open [rs2 (.executeQuery stmt2)]
                (if (.next rs2)
                  (let [model (.getString rs2 "str_value")]
                    (get model->dimension model 384))
                  ;; No model metadata — check known collections, default 384
                  (get collection->known-dimension collection-name 384))))))))))

(defn connect-target
  "Step 4: Connect to Milvus and ensure collection exists with correct dimension."
  [{:keys [milvus-host milvus-port milvus-collection sqlite-path chroma-collection] :as ctx}]
  (r/try-effect* :target/connect-failed
    (require '[milvus-clj.api :as milvus]
             '[milvus-clj.schema :as milvus-schema]
             '[milvus-clj.index :as milvus-index])
    ((requiring-resolve 'milvus-clj.api/connect!)
     {:host milvus-host :port milvus-port})
    (let [dim (detect-dimension sqlite-path chroma-collection)]
      (when-not @((requiring-resolve 'milvus-clj.api/has-collection) milvus-collection)
        (log/info "Creating collection" milvus-collection "with dimension" dim)
        @((requiring-resolve 'milvus-clj.api/create-collection) milvus-collection
          {:schema ((requiring-resolve 'milvus-clj.schema/with-dimension) dim)
           :index  @(requiring-resolve 'milvus-clj.index/default-memory-index)
           :description (str "Migrated from Chroma: " chroma-collection)}))
      (assoc ctx :dimension dim))))

;; =============================================================================
;; Batch Processing
;; =============================================================================

(defn embed-entry
  "Embed a single entry. Empty content gets a zero vector (preserves ID for KG).
   Returns {:record ... :skipped? false} or {:skipped? true :id ...}."
  [entry ollama-host model dimension]
  (let [text (or (:content entry) "")]
    (if (empty? text)
      ;; Zero vector — entry exists in Milvus for KG reference, but won't match searches
      {:skipped? false :record (entry->record entry (vec (repeat dimension 0.0)))}
      (let [result (embed-text text ollama-host model)]
        (if (r/ok? result)
          {:skipped? false :record (entry->record entry (:ok result))}
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

            (write-cursor! cursor-path
                           (assoc cursor
                             :completed-batches (inc batch-idx)
                             :inserted @inserted
                             :skipped @skipped
                             :last-batch-at (now-iso)))

            (log/info (format "Batch %d/%d (%d/%d) %dms [%d inserted, %d skipped]"
                              (inc batch-idx) (+ batch-base (count batches))
                              done total elapsed @inserted @skipped))))

        (let [total-ms (- (System/currentTimeMillis) start-ms)
              final    {:status :completed
                        :completed-batches (+ batch-base (count batches))
                        :inserted @inserted :skipped @skipped
                        :completed-at (now-iso) :last-batch-at (now-iso)}]
          (write-cursor! cursor-path (merge cursor final))
          (log/info "=== Migration complete ==="
                    {:total total :inserted @inserted :skipped @skipped
                     :elapsed-ms total-ms
                     :rate (format "%.1f entries/sec"
                                   (/ (* (- @inserted (or (:inserted cursor) 0)) 1000.0)
                                      (max total-ms 1)))})
          final)))))

;; =============================================================================
;; Multi-Collection: List & Filter
;; =============================================================================

(def ^:private test-collection-patterns
  "Collections matching these patterns are skipped in --all mode."
  [#".*-test$" #"^test-" #".*-e2e-.*" #".*-test-.*"])

(defn- test-collection? [name]
  (some #(re-matches % name) test-collection-patterns))

(defn list-collections
  "List all collections from Chroma SQLite with entry counts.
   Returns vec of {:name :entry-count}."
  [sqlite-path]
  (with-open [conn (get-connection sqlite-path)]
    (with-open [stmt (.prepareStatement conn
                       "SELECT c.name, COUNT(DISTINCT e.embedding_id) as cnt
                        FROM collections c
                        JOIN segments s ON s.collection = c.id AND s.scope = 'METADATA'
                        JOIN embeddings e ON e.segment_id = s.id
                        GROUP BY c.name
                        ORDER BY cnt DESC")]
      (with-open [rs (.executeQuery stmt)]
        (loop [result []]
          (if (.next rs)
            (recur (conj result {:name (.getString rs "name")
                                 :entry-count (.getInt rs "cnt")}))
            result))))))

(defn- collection->milvus-name
  "Convert Chroma collection name to Milvus-safe name (underscores, no hyphens)."
  [name]
  (str/replace name "-" "_"))

(defn- cursor-path-for
  "Derive per-collection cursor path from base cursor path."
  [base-cursor-path collection-name]
  (let [dir  (or (.getParent (io/file base-cursor-path)) ".")
        safe (str/replace collection-name "/" "_")]
    (str dir "/cursor-" safe ".edn")))

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
                   (r/bind migrate-batches!))]
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
      (let [{:keys [sqlite-path cursor-path] :as config} (:ok config-result)
            all-cols   (or (:collections overrides)
                           (->> (list-collections sqlite-path)
                                (remove #(test-collection? (:name %)))
                                (remove #(and (not (:include-empty? overrides))
                                              (zero? (:entry-count %))))
                                (mapv :name)))
            results    (atom [])]
        (log/info "=== Multi-collection migration ===" {:collections (count all-cols)})
        (doseq [coll-name all-cols]
          (let [milvus-name (collection->milvus-name coll-name)
                coll-cursor (cursor-path-for cursor-path coll-name)
                coll-overrides (merge overrides
                                      {:chroma-collection coll-name
                                       :milvus-collection milvus-name
                                       :cursor-path       coll-cursor})]
            (log/info "--- Starting collection:" coll-name "→" milvus-name "---")
            (let [dim (detect-dimension sqlite-path coll-name)
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
            collections (or (:collections overrides) ["hive-mcp-memory"])
            results (atom [])]
        (doseq [coll-name collections]
          (let [milvus-name (collection->milvus-name coll-name)
                dim (detect-dimension sqlite-path coll-name)]
            (if (nil? dim)
              (do (log/info "Skipping" coll-name "(noop embedding)")
                  (swap! results conj {:collection coll-name :status :skipped}))
              (let [sync-result
                    (r/try-effect* :sync/failed
                      (log/info "Syncing" coll-name "→" milvus-name)
                      (let [connect-fn (requiring-resolve 'milvus-clj.api/connect!)
                            _           (connect-fn {:host (:milvus-host config)
                                                     :port (:milvus-port config)})
                            all-entries  (read-all-entries sqlite-path coll-name)
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
                            (doseq [[i batch] (map-indexed vector batches)]
                              (log/info "Batch" (inc i) "/" (count batches)
                                        "(" (count batch) "entries)")
                              (let [r (process-batch! (vec batch) ollama-host embed-model
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
        (let [summary {:collections (count collections) :results @results}]
          (log/info "Sync complete:" summary)
          summary)))))

