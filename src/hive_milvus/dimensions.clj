(ns hive-milvus.dimensions
  "Detect embedding dimension for Chroma collections."
  (:require [hive-dsl.result :as r]
            [hive-milvus.sqlite :as sqlite]))

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

(defn detect-dimension
  "Detect embedding dimension for a Chroma collection.
   Checks: explicit 'dimension' metadata → 'embedding-model' metadata → default 384."
  [sqlite-path collection-name]
  (r/rescue 384
    (with-open [conn (sqlite/get-connection sqlite-path)]
      (with-open [stmt (.prepareStatement conn
                         "SELECT cm.int_value FROM collection_metadata cm
                          JOIN collections c ON c.id = cm.collection_id
                          WHERE c.name = ? AND cm.key = 'dimension'")]
        (.setString stmt 1 collection-name)
        (with-open [rs (.executeQuery stmt)]
          (if (and (.next rs) (pos? (.getInt rs "int_value")))
            (.getInt rs "int_value")
            (with-open [stmt2 (.prepareStatement conn
                                "SELECT cm.str_value FROM collection_metadata cm
                                 JOIN collections c ON c.id = cm.collection_id
                                 WHERE c.name = ? AND cm.key = 'embedding-model'")]
              (.setString stmt2 1 collection-name)
              (with-open [rs2 (.executeQuery stmt2)]
                (if (.next rs2)
                  (let [model (.getString rs2 "str_value")]
                    (get model->dimension model 384))
                  (get collection->known-dimension collection-name 384))))))))))
