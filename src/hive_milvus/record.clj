(ns hive-milvus.record
  "Pure: memory entry → Milvus insert record."
  (:require [clojure.data.json :as json]))

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
