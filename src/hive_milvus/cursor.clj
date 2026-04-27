(ns hive-milvus.cursor
  "Persistent resume state for batch migrations."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

(defn now-iso []
  (str (java.time.ZonedDateTime/now (java.time.ZoneId/systemDefault))))

(defn read-cursor [cursor-path]
  (when (.exists (io/file cursor-path))
    (try (edn/read-string (slurp cursor-path))
         (catch Exception e
           (log/warn "Corrupt cursor, starting fresh:" (.getMessage e))
           nil))))

(defn write-cursor! [cursor-path cursor]
  (let [tmp (str cursor-path ".tmp")]
    (spit tmp (pr-str cursor))
    (.renameTo (io/file tmp) (io/file cursor-path))))

(defn fresh-cursor [total batch-size]
  {:status :running, :completed-batches 0, :batch-size batch-size,
   :total-entries total, :inserted 0, :skipped 0,
   :started-at (now-iso), :last-batch-at nil})

(defn cursor-path-for
  "Derive per-collection cursor path from base cursor path."
  [base-cursor-path collection-name]
  (let [dir  (or (.getParent (io/file base-cursor-path)) ".")
        safe (str/replace collection-name "/" "_")]
    (str dir "/cursor-" safe ".edn")))
