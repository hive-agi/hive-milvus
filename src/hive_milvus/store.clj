(ns hive-milvus.store
  "Milvus implementation of IMemoryStore protocol.

   Standalone addon project — hive-mcp depends on this via IAddon,
   never the reverse. Wraps milvus-clj.api into protocol methods.

   Key difference from Chroma: Milvus requires explicit embeddings on
   insert and search. We use hive-mcp.embeddings.service to generate
   them — the same provider chain as Chroma.

   DDD: Repository pattern — MilvusMemoryStore is the Milvus aggregate adapter."
  (:require [hive-mcp.protocols.memory :as proto]
            [hive-mcp.embeddings.service :as embed-svc]
            [milvus-clj.api :as milvus]
            [milvus-clj.schema :as milvus-schema]
            [milvus-clj.index :as milvus-index]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

;; =========================================================================
;; Pure Calculations
;; =========================================================================

(defn- now-iso
  "Current ISO 8601 timestamp string."
  []
  (str (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))))

(defn staleness-probability
  "Calculate staleness probability from alpha/beta parameters."
  [entry]
  (let [alpha (or (:staleness-alpha entry) 1)
        beta  (or (:staleness-beta entry) 1)]
    (/ (double beta) (+ alpha beta))))

(defn- feedback->field
  "Map feedback keyword to entry field name."
  [feedback]
  (case feedback
    :helpful   :helpful-count
    :unhelpful :unhelpful-count))

(defn helpfulness-map
  "Build helpfulness ratio map from entry counts."
  [entry]
  (let [helpful   (or (:helpful-count entry) 0)
        unhelpful (or (:unhelpful-count entry) 0)
        total     (+ helpful unhelpful)]
    {:helpful-count   helpful
     :unhelpful-count unhelpful
     :total           total
     :ratio           (if (pos? total) (double (/ helpful total)) 0.0)}))

;; =========================================================================
;; Entry <-> Milvus Record Conversion
;; =========================================================================

(defn tags->str
  "Serialize tags to JSON string for Milvus VarChar field."
  [tags]
  (if (sequential? tags)
    (json/write-str tags)
    (or (str tags) "[]")))

(defn str->tags
  "Deserialize tags from JSON string."
  [s]
  (when (and s (not (str/blank? s)))
    (try (json/read-str s :key-fn keyword)
         (catch Exception _ []))))

(defn- entry->record
  "Convert a memory entry map to a Milvus insert record.
   Generates embedding via the embedding service."
  [entry collection-name]
  (let [content   (or (:content entry) "")
        embedding (embed-svc/embed-for-collection collection-name content)]
    {:id              (or (:id entry) (proto/generate-id))
     :embedding       embedding
     :document        (or (:content entry) "")
     :type            (or (some-> (:type entry) name) "note")
     :tags            (tags->str (:tags entry))
     :content         content
     :content_hash    (or (:content-hash entry) "")
     :created         (or (:created entry) (now-iso))
     :updated         (or (:updated entry) (now-iso))
     :duration        (or (some-> (:duration entry) name) "medium")
     :expires         (or (:expires entry) "")
     :access_count    (or (:access-count entry) 0)
     :helpful_count   (or (:helpful-count entry) 0)
     :unhelpful_count (or (:unhelpful-count entry) 0)
     :project_id      (or (:project-id entry) "")}))

(defn record->entry
  "Convert a Milvus query result row to a memory entry map."
  [row]
  (let [tags-raw (:tags row)]
    (cond-> {:id              (:id row)
             :type            (keyword (or (:type row) "note"))
             :content         (or (:content row) (:document row) "")
             :tags            (str->tags tags-raw)
             :content-hash    (:content_hash row)
             :created         (:created row)
             :updated         (:updated row)
             :duration        (keyword (or (:duration row) "medium"))
             :expires         (when-let [e (:expires row)]
                                (when-not (str/blank? e) e))
             :access-count    (or (:access_count row) 0)
             :helpful-count   (or (:helpful_count row) 0)
             :unhelpful-count (or (:unhelpful_count row) 0)
             :project-id      (let [pid (:project_id row)]
                                (when-not (str/blank? pid) pid))}
      (:distance row) (assoc :distance (:distance row)))))

;; =========================================================================
;; Filter Expression Builders
;; =========================================================================

(defn build-filter-expr
  "Build a Milvus boolean filter expression from query opts."
  [{:keys [type project-id project-ids tags exclude-tags include-expired?]}]
  (let [clauses (cond-> []
                  type
                  (conj (str "type == \"" (name type) "\""))

                  project-id
                  (conj (str "project_id == \"" project-id "\""))

                  (and project-ids (seq project-ids))
                  (conj (str "project_id in ["
                             (str/join ", " (map #(str "\"" % "\"") project-ids))
                             "]"))

                  (and tags (seq tags))
                  (into (map (fn [t] (str "tags like \"%" t "%\"")) tags))

                  (and exclude-tags (seq exclude-tags))
                  (into (map (fn [t] (str "tags not like \"%" t "%\"")) exclude-tags))

                  (not include-expired?)
                  (conj (str "(expires == \"\" or expires > \""
                             (now-iso) "\")")))]
    (when (seq clauses)
      (str/join " and " clauses))))

;; =========================================================================
;; Milvus Helpers
;; =========================================================================

(defn- ensure-collection!
  "Ensure the memory collection exists, creating it if needed."
  [collection-name dimension]
  (when-not @(milvus/has-collection collection-name)
    (log/info "Creating Milvus collection:" collection-name "dim:" dimension)
    @(milvus/create-collection collection-name
       {:schema (milvus-schema/with-dimension dimension)
        :index  milvus-index/default-memory-index
        :description "hive-mcp memory store"})))

(defn- get-entry-by-id
  "Fetch a single entry by ID from Milvus. Returns entry map or nil."
  [collection-name id]
  (let [results @(milvus/get collection-name [id])]
    (when (seq results)
      (record->entry (first results)))))

(defn- update-entry-fields!
  "Update an entry by reading, merging, and upserting."
  [collection-name id updates]
  (when-let [existing (get-entry-by-id collection-name id)]
    (let [merged (merge existing updates)
          record (entry->record (assoc merged :id id :updated (now-iso))
                                collection-name)]
      @(milvus/add collection-name [record] :upsert? true)
      merged)))

;; =========================================================================
;; Protocol Implementation
;; =========================================================================

(defrecord MilvusMemoryStore [config-atom]
  ;; =========================================================================
  ;; IMemoryStore - Core Protocol
  ;; =========================================================================
  proto/IMemoryStore

  ;; --- Connection Lifecycle ---

  (connect! [_this config]
    (try
      (let [milvus-config (select-keys config [:host :port :token :database :secure])
            coll-name     (or (:collection-name config) "hive-mcp-memory")]
        (swap! config-atom merge config)
        (milvus/connect! milvus-config)
        (let [dimension (embed-svc/get-dimension-for coll-name)]
          (ensure-collection! coll-name dimension))
        {:success? true
         :backend  "milvus"
         :errors   []
         :metadata (select-keys @config-atom [:host :port :collection-name])})
      (catch Exception e
        {:success? false
         :backend  "milvus"
         :errors   [(.getMessage e)]
         :metadata {}})))

  (disconnect! [_this]
    (try
      (milvus/disconnect!)
      {:success? true :errors []}
      (catch Exception e
        {:success? false :errors [(.getMessage e)]})))

  (connected? [_this]
    (milvus/connected?))

  (health-check [_this]
    (let [start-ms (System/currentTimeMillis)
          ts       (now-iso)]
      (try
        (let [coll-name (:collection-name @config-atom "hive-mcp-memory")
              exists?   @(milvus/has-collection coll-name)
              latency   (- (System/currentTimeMillis) start-ms)]
          {:healthy?    exists?
           :latency-ms  latency
           :backend     "milvus"
           :entry-count nil
           :errors      (if exists? [] ["Collection does not exist"])
           :checked-at  ts})
        (catch Exception e
          {:healthy?    false
           :latency-ms  (- (System/currentTimeMillis) start-ms)
           :backend     "milvus"
           :entry-count nil
           :errors      [(.getMessage e)]
           :checked-at  ts}))))

  ;; --- CRUD Operations ---

  (add-entry! [_this entry]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")
          record    (entry->record entry coll-name)]
      @(milvus/add coll-name [record] :upsert? true)
      (:id record)))

  (get-entry [_this id]
    (get-entry-by-id (:collection-name @config-atom "hive-mcp-memory") id))

  (update-entry! [_this id updates]
    (update-entry-fields! (:collection-name @config-atom "hive-mcp-memory")
                          id updates))

  (delete-entry! [_this id]
    @(milvus/delete (:collection-name @config-atom "hive-mcp-memory") [id])
    true)

  (query-entries [_this opts]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")
          {:keys [limit] :or {limit 100}} opts
          filter-expr (build-filter-expr opts)
          results (if filter-expr
                    @(milvus/query-scalar coll-name
                       {:filter filter-expr :limit limit})
                    @(milvus/query-scalar coll-name
                       {:filter "id != \"\"" :limit limit}))]
      (mapv record->entry results)))

  ;; --- Semantic Search ---

  (search-similar [_this query-text opts]
    (let [coll-name   (:collection-name @config-atom "hive-mcp-memory")
          {:keys [limit type project-ids exclude-tags]
           :or   {limit 10}} opts
          query-vec   (embed-svc/embed-for-collection coll-name query-text)
          filter-expr (build-filter-expr
                        (cond-> {:include-expired? false}
                          type        (assoc :type type)
                          project-ids (assoc :project-ids project-ids)
                          exclude-tags (assoc :exclude-tags exclude-tags)))
          results     @(milvus/query coll-name
                         (cond-> {:vector query-vec :limit limit}
                           filter-expr (assoc :filter filter-expr)))]
      (mapv record->entry results)))

  (supports-semantic-search? [_this]
    (embed-svc/provider-available-for?
      (:collection-name @config-atom "hive-mcp-memory")))

  ;; --- Expiration Management ---

  (cleanup-expired! [_this]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")
          now       (now-iso)
          expired   @(milvus/query-scalar coll-name
                       {:filter (str "expires != \"\" and expires < \"" now "\"")
                        :output-fields ["id"]
                        :limit 10000})]
      (when (seq expired)
        (let [ids (mapv :id expired)]
          @(milvus/delete coll-name ids)
          (log/info "Cleaned up" (count ids) "expired entries from Milvus")))
      (count expired)))

  (entries-expiring-soon [_this days opts]
    (let [coll-name  (:collection-name @config-atom "hive-mcp-memory")
          now        (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))
          horizon    (str (.plusDays now days))
          now-str    (str now)
          filter-cls (cond-> [(str "expires != \"\" and expires > \"" now-str
                               "\" and expires < \"" horizon "\"")]
                       (:project-id opts)
                       (conj (str "project_id == \"" (:project-id opts) "\"")))
          results    @(milvus/query-scalar coll-name
                        {:filter (str/join " and " filter-cls)
                         :limit 1000})]
      (mapv record->entry results)))

  ;; --- Duplicate Detection ---

  (find-duplicate [_this type content-hash opts]
    (let [coll-name  (:collection-name @config-atom "hive-mcp-memory")
          filter-cls (cond-> [(str "type == \"" (name type) "\"")
                              (str "content_hash == \"" content-hash "\"")]
                       (:project-id opts)
                       (conj (str "project_id == \"" (:project-id opts) "\"")))
          results    @(milvus/query-scalar coll-name
                        {:filter (str/join " and " filter-cls)
                         :limit 1})]
      (when (seq results)
        (record->entry (first results)))))

  ;; --- Store Management ---

  (store-status [_this]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")]
      {:backend          "milvus"
       :configured?      (milvus/connected?)
       :entry-count      (try
                           (count @(milvus/query-scalar coll-name
                                     {:filter "id != \"\""
                                      :output-fields ["id"]
                                      :limit 100000}))
                           (catch Exception _ nil))
       :supports-search? (milvus/connected?)}))

  (reset-store! [_this]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")]
      (when @(milvus/has-collection coll-name)
        @(milvus/drop-collection coll-name))
      true))

  ;; =========================================================================
  ;; IMemoryStoreWithAnalytics
  ;; =========================================================================
  proto/IMemoryStoreWithAnalytics

  (log-access! [_this id]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")]
      (when-let [entry (get-entry-by-id coll-name id)]
        (update-entry-fields! coll-name id
          {:access-count (inc (or (:access-count entry) 0))}))))

  (record-feedback! [_this id feedback]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")]
      (when-let [entry (get-entry-by-id coll-name id)]
        (let [field     (feedback->field feedback)
              new-count (inc (or (get entry field) 0))]
          (update-entry-fields! coll-name id {field new-count})))))

  (get-helpfulness-ratio [_this id]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")]
      (when-let [entry (get-entry-by-id coll-name id)]
        (helpfulness-map entry))))

  ;; =========================================================================
  ;; IMemoryStoreWithStaleness
  ;; =========================================================================
  proto/IMemoryStoreWithStaleness

  (update-staleness! [_this id staleness-opts]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")
          {:keys [beta source depth]} staleness-opts]
      (update-entry-fields! coll-name id
        (cond-> {}
          beta   (assoc :staleness-beta beta)
          source (assoc :staleness-source (name source))
          depth  (assoc :staleness-depth depth)))))

  (get-stale-entries [_this threshold opts]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")
          {:keys [project-id type]} opts
          filter-expr (build-filter-expr {:project-id project-id :type type
                                          :include-expired? true})
          results @(milvus/query-scalar coll-name
                     {:filter (or filter-expr "id != \"\"")
                      :limit 10000})]
      (->> (mapv record->entry results)
           (filter #(> (staleness-probability %) threshold))
           vec)))

  (propagate-staleness! [_this source-id depth]
    (let [coll-name (:collection-name @config-atom "hive-mcp-memory")]
      (when-let [entry (get-entry-by-id coll-name source-id)]
        (let [kg-outgoing (:kg-outgoing entry)]
          (reduce (fn [cnt dep-id]
                    (if (and dep-id (seq dep-id))
                      (if-let [dep (get-entry-by-id coll-name dep-id)]
                        (do (update-entry-fields! coll-name dep-id
                              {:staleness-beta (inc (or (:staleness-beta dep) 1))
                               :staleness-source "transitive"
                               :staleness-depth (inc depth)})
                            (inc cnt))
                        cnt)
                      cnt))
                  0
                  kg-outgoing))))))

(defn create-store
  "Create a new Milvus-backed memory store.

   Options (also configurable via connect!):
     :host            - Milvus gRPC host (default: localhost)
     :port            - Milvus gRPC port (default: 19530)
     :collection-name - Collection name (default: hive-mcp-memory)

   Returns an IMemoryStore implementation.

   Example:
     (def store (create-store {:host \"milvus.milvus.svc\" :port 19530}))
     (proto/connect! store {:host \"milvus.milvus.svc\" :port 19530})
     (proto/set-store! store)"
  ([]
   (create-store {}))
  ([opts]
   (log/info "Creating MilvusMemoryStore" (when (seq opts) opts))
   (->MilvusMemoryStore (atom (merge {:host "localhost"
                                       :port 19530
                                       :collection-name "hive-mcp-memory"}
                                      opts)))))
