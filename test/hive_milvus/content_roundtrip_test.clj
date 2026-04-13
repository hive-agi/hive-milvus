(ns hive-milvus.content-roundtrip-test
  "Tests for content serialization roundtrip in MilvusMemoryStore.

   Catches the bug where record->entry returned raw JSON strings instead
   of parsed maps for structured content (e.g. kanban tasks with :task-type).
   Chroma's metadata->entry called try-parse-json; Milvus's record->entry
   did not — breaking any code that called (get content :task-type)."
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [clojure.data.json :as json]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-test.properties :as props]
            [hive-milvus.store :as milvus-store]))

;; =============================================================================
;; Generators
;; =============================================================================

(def gen-milvus-row-with-json-content
  "Generator for Milvus rows whose :content is a JSON-encoded map."
  (gen/let [title gen/string-alphanumeric
            status (gen/elements ["todo" "doing" "review" "done"])
            priority (gen/elements ["high" "medium" "low"])
            task-type (gen/elements ["kanban" "note" "plan"])]
    {:id "gen-test"
     :type "note"
     :content (json/write-str {:task-type task-type
                                :title title
                                :status status
                                :priority priority})
     :tags "[\"kanban\"]"
     :content_hash ""
     :created "2026-04-08T00:00:00Z"
     :updated "2026-04-08T00:00:00Z"
     :duration "short"
     :expires ""
     :access_count 0
     :helpful_count 0
     :unhelpful_count 0
     :project_id "test"}))

(def gen-milvus-row-plain-string
  "Generator for Milvus rows whose :content is a plain string."
  (gen/let [content gen/string-alphanumeric]
    {:id "gen-plain"
     :type "decision"
     :content content
     :tags "[]"
     :content_hash ""
     :created "" :updated ""
     :duration "medium"
     :expires ""
     :access_count 0
     :helpful_count 0
     :unhelpful_count 0
     :project_id ""}))

(def gen-milvus-row-any
  "Generator for any Milvus row (JSON map or plain string content)."
  (gen/one-of [gen-milvus-row-with-json-content
               gen-milvus-row-plain-string]))

;; =============================================================================
;; 1. Trifecta: record->entry content deserialization
;;    Golden + property + mutation — the core bug surface.
;; =============================================================================

(deftrifecta content-deserialization
  hive-milvus.store/record->entry
  {:golden-path "test/golden/milvus/content-deserialization.edn"
   :cases       {:json-map     {:id "t-map" :type "note"
                                 :content "{\"task-type\":\"kanban\",\"title\":\"Fix bug\",\"status\":\"todo\"}"
                                 :tags "[\"kanban\",\"todo\"]"
                                 :content_hash "abc" :created "2026-04-08T10:00:00Z"
                                 :updated "2026-04-08T10:00:00Z" :duration "short"
                                 :expires "" :access_count 0
                                 :helpful_count 0 :unhelpful_count 0
                                 :project_id "hive-mcp"}
                 :json-array   {:id "t-arr" :type "note"
                                 :content "[\"a\",\"b\",\"c\"]"
                                 :tags "[]" :content_hash ""
                                 :created "" :updated "" :duration "medium"
                                 :expires "" :access_count 0
                                 :helpful_count 0 :unhelpful_count 0
                                 :project_id ""}
                 :plain-string {:id "t-plain" :type "decision"
                                 :content "Just a plain decision."
                                 :tags "[]" :content_hash ""
                                 :created "" :updated "" :duration "long"
                                 :expires "" :access_count 0
                                 :helpful_count 0 :unhelpful_count 0
                                 :project_id ""}
                 :empty-string {:id "t-empty" :type "note"
                                 :content ""
                                 :tags "[]" :content_hash ""
                                 :created "" :updated "" :duration "medium"
                                 :expires "" :access_count 0
                                 :helpful_count 0 :unhelpful_count 0
                                 :project_id ""}}
   :gen         gen-milvus-row-any
   :pred        #(and (map? %)
                      (contains? % :content)
                      (contains? % :id)
                      ;; Content must never be a JSON-looking string — it should be parsed
                      (not (and (string? (:content %))
                                (or (clojure.string/starts-with? (:content %) "{")
                                    (clojure.string/starts-with? (:content %) "[")))))
   :num-tests   200
   :mutations   [["no-parse" (fn [row]
                                ;; Simulates the old bug: return content as raw string
                                (assoc (milvus-store/record->entry row)
                                       :content (or (:content row) (:document row) "")))]]})

;; =============================================================================
;; 2. Trifecta: kanban-task-type accessibility after roundtrip
;;    Verifies the kanban-specific check works on deserialized content.
;; =============================================================================

(defn- kanban-task-type?
  "Mirror of memory_kanban.clj's check — the exact code path that broke."
  [content]
  (some #(= "kanban" (get content %)) [:task-type "task-type"]))

(deftrifecta kanban-content-access
  hive-milvus.store/record->entry
  {:golden-path "test/golden/milvus/kanban-content-access.edn"
   :cases       {:kanban-task {:id "kb-1" :type "note"
                                :content "{\"task-type\":\"kanban\",\"title\":\"Deploy\",\"status\":\"doing\"}"
                                :tags "[\"kanban\",\"doing\"]"
                                :content_hash "" :created "" :updated ""
                                :duration "short" :expires ""
                                :access_count 0 :helpful_count 0
                                :unhelpful_count 0 :project_id "hive-mcp"}}
   :xf          (fn [entry] (kanban-task-type? (:content entry)))
   :gen         gen-milvus-row-with-json-content
   :pred        (fn [entry] (kanban-task-type? (:content entry)))
   :num-tests   100
   :mutations   [["raw-string-content"
                  (fn [row]
                    ;; Old bug: content stays as string, get returns nil
                    (assoc (milvus-store/record->entry row)
                           :content (:content row)))]]})

;; =============================================================================
;; 3. Roundtrip property: JSON map → write-str → record->entry → same map
;;    Uses defprop-roundtrip from hive-test.
;; =============================================================================

(def gen-kanban-content-map
  "Generator for kanban-like content maps."
  (gen/let [title gen/string-alphanumeric
            status (gen/elements ["todo" "doing" "review" "done"])
            priority (gen/elements ["high" "medium" "low"])]
    {:task-type "kanban" :title title :status status :priority priority}))

(props/defprop-roundtrip content-json-roundtrip
  json/write-str
  (fn [json-str]
    (:content (milvus-store/record->entry
                {:id "rt" :type "note" :content json-str
                 :tags "[]" :content_hash "" :created "" :updated ""
                 :duration "medium" :expires ""
                 :access_count 0 :helpful_count 0 :unhelpful_count 0
                 :project_id ""})))
  gen-kanban-content-map
  {:num-tests 100})
