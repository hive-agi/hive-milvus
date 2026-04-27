(ns hive-milvus.sqlite
  "Chroma SQLite reader — pure JDBC access, no domain logic."
  (:require [clojure.string :as str])
  (:import [java.sql DriverManager]))

(defn get-connection [sqlite-path]
  (Class/forName "org.sqlite.JDBC")
  (DriverManager/getConnection (str "jdbc:sqlite:" sqlite-path)))

(defn get-collection-segment-id [conn collection-name]
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
