(ns hive-milvus.store.schema
  "Entry <-> Milvus record conversion and filter expression builders.

   Pure functions only — no side effects, no Milvus RPCs. Extracted from
   hive-milvus.store so the schema/translation layer can be understood
   (and tested) independently of the connection + resilience machinery."
  (:require [hive-mcp.protocols.memory :as proto]
            [hive-mcp.embeddings.service :as embed-svc]
            [hive-mcp.dns.result :as r]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [hive-milvus.embedder :as embedder]))

;; =========================================================================
;; Pure Calculations
;; =========================================================================

(defn now-iso
  "Current ISO 8601 timestamp string."
  []
  (str (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))))

(defn staleness-probability
  "Calculate staleness probability from alpha/beta parameters."
  [entry]
  (let [alpha (or (:staleness-alpha entry) 1)
        beta  (or (:staleness-beta entry) 1)]
    (/ (double beta) (+ alpha beta))))

(defn feedback->field
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

(defn entry->record-pure
  "PURE: Convert a memory entry + a precomputed embedding vector into a
   Milvus insert record. Performs NO embedding lookup, NO provider
   resolution, NO I/O — the caller is responsible for producing the
   embedding via `hive-milvus.embedder/embed-for-entry` first.

   Splits CPPB stages: this fn is PROMOTE only; the embedding side of
   the record (BOUNDARY) lives in `hive-milvus.embedder`. Callers that
   skip the embed step ship a record with an empty vector (suitable
   for tests / id-only updates that intentionally don't re-vectorize)."
  [entry _collection-name embedding]
  (let [raw-content (or (:content entry) "")
        content     (if (map? raw-content) (json/write-str raw-content) (str raw-content))]
    {:id              (or (:id entry) (proto/generate-id))
     :embedding       (or embedding [])
     :document        ""
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

(defn entry->record
  "LEGACY FACADE — invokes the BOUNDARY embedder then builds the record.
   Throws if the embedder fails. Prefer the explicit two-step path:

     (let [emb-res (embedder/embed-for-entry entry coll)]
       (if (r/ok? emb-res)
         (entry->record-pure entry coll (:ok emb-res))
         (handle-err emb-res)))

   so embedding failures stay railway-tracked instead of escaping as
   exceptions. This 2-arg arity is retained for back-compat with
   pre-CPPB-refactor call sites."
  [entry collection-name]
  (let [embedding (embedder/embed-for-entry-or-throw entry collection-name)]
    (entry->record-pure entry collection-name embedding)))

(defn try-parse-json
  "Parse JSON string to map/vec, return original on failure."
  [s]
  (if (and (string? s)
           (or (str/starts-with? s "{") (str/starts-with? s "[")))
    (r/rescue s (json/read-str s :key-fn keyword))
    s))

(defn record->entry
  "Convert a Milvus query result row to a memory entry map."
  [row]
  (let [tags-raw (:tags row)
        raw-content (or (:content row) (:document row) "")]
    (cond-> {:id              (:id row)
             :type            (keyword (or (:type row) "note"))
             :content         (try-parse-json raw-content)
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

(def default-read-fields
  ;; Projection for all bulk reads. Omits `embedding` (huge float vector)
  ;; and `document` (byte-equal duplicate of `content`). See
  ;; "Milvus projection pruning" + "document duplicates content" conventions.
  ["id" "type" "tags" "content" "content_hash" "created" "updated"
   "duration" "expires" "access_count" "helpful_count" "unhelpful_count"
   "project_id"])

(def metadata-read-fields
  ;; Metadata-only projection: drops `content` (slow over HTTP). Per
  ;; experiment 20260416235146-39f2795f, full projection 100 rows = 53.8s
  ;; vs id-only 3.0s. Callers that only need id/type/tags/timestamps for
  ;; filtering + ordering should pass :output-fields metadata-read-fields.
  ;; Content gets re-hydrated via IMemoryStoreBatch get-entries for top-N.
  ["id" "type" "tags" "content_hash" "created" "updated"
   "duration" "expires" "access_count" "helpful_count" "unhelpful_count"
   "project_id"])

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
                  (conj (if (= 1 (count project-ids))
                          ;; Equality uses the INVERTED index better than
                          ;; `in [single-item]` on Milvus HTTP — observed
                          ;; ~2x speedup on scope-filtered catchup scans.
                          (str "project_id == \"" (first project-ids) "\"")
                          (str "project_id in ["
                               (str/join ", " (map #(str "\"" % "\"") project-ids))
                               "]")))

                  (and tags (seq tags))
                  (into (map (fn [t] (str "tags like \"%" t "%\"")) tags))

                  (and exclude-tags (seq exclude-tags))
                  (into (map (fn [t] (str "not (tags like \"%" t "%\")")) exclude-tags))

                  (not include-expired?)
                  (conj (str "(expires == \"\" or expires > \""
                             (now-iso) "\")")))]
    (when (seq clauses)
      (str/join " and " clauses))))
