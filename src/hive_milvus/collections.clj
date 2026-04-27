(ns hive-milvus.collections
  "Multi-collection helpers: filtering, naming."
  (:require [clojure.string :as str]))

(def ^:private test-collection-patterns
  "Collections matching these patterns are skipped in --all mode."
  [#".*-test$" #"^test-" #".*-e2e-.*" #".*-test-.*"])

(defn test-collection? [name]
  (some #(re-matches % name) test-collection-patterns))

(defn collection->milvus-name
  "Convert Chroma collection name to Milvus-safe name (underscores, no hyphens)."
  [name]
  (str/replace name "-" "_"))
