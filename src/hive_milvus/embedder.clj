;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.embedder
  "BOUNDARY: re-embed an entry's content via the injected embedding port.
   Record-shape conversion stays pure in `hive-milvus.store.schema`; the
   provider lookup + remote embed live behind `hive-milvus.embed.port`.

   `embed-for-entry` policy (port-agnostic):
     blank content => (r/ok [])
     no-embed type => (r/ok zero-vector) sized to the type's routed dim
     otherwise     => `port/embed-entry`, then dim-guarded.

   Returns (r/ok vec) or one of
     :embedder/no-provider  :embedder/embed-failed  :embedder/dim-mismatch."
  (:require [clojure.string :as str]
            [clojure.data.json :as json]
            [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]
            [hive-milvus.collection.naming :as naming]
            [taoensso.timbre :as log]))

(defn- ^String entry->content
  "Coerce the entry's :content field to a string. Maps are JSON-encoded."
  [entry]
  (let [raw (or (:content entry) "")]
    (if (map? raw)
      (try (json/write-str raw)
           (catch Exception _ (pr-str raw)))
      (str raw))))

(defn- guard-dim
  "Reject an embedding whose width is not the target collection's (Milvus
   would 1804, or a same-width foreign vector would index as noise). Empty
   vectors pass: the caller decides what to do with blank content."
  [res collection-name entry]
  (if (and (r/ok? res) (seq (:ok res)))
    (let [expected (naming/dim-of collection-name)
          actual   (count (:ok res))]
      (if (and expected (not= expected actual))
        (do (log/error "embed-for-entry: dimension mismatch"
                       {:collection collection-name :expected expected :actual actual})
            (r/err :embedder/dim-mismatch
                   {:collection collection-name
                    :type       (:type entry)
                    :expected   expected
                    :actual     actual}))
        res))
    res))

(defn- embed-for-entry*
  [entry collection-name]
  (let [content (entry->content entry)]
    (cond
      (str/blank? content)
      (r/ok [])

      (port/no-embed-type? (:type entry))
      (let [dim (try (:dimension (port/routing-for-type (:type entry)))
                     (catch Exception _ 768))]
        (r/ok (vec (repeat (or dim 768) 0.0))))

      :else
      (port/embed-entry entry collection-name content))))

(defn embed-for-entry
  "Re-embed `entry`'s content for `collection-name` via the injected port.

   Returns:
     (r/ok embedding-vec)                on success
     (r/err :embedder/no-provider {…})   when neither type nor coll resolves
     (r/err :embedder/embed-failed {…})  when the embed call throws
     (r/err :embedder/dim-mismatch {…})  when the vector's width is not the
                                         one `collection-name` holds

   Side-effect: one provider HTTP call (inside the port). Caller passes the
   resulting vector to `schema/entry->record`."
  [entry collection-name]
  (-> (embed-for-entry* entry collection-name)
      (guard-dim collection-name entry)))

(defn embed-for-entry-or-throw
  "Convenience for legacy call sites that expect an embedding vector (or a
   throw). Prefer `embed-for-entry` in new code so errors stay railway-tracked."
  [entry collection-name]
  (let [res (embed-for-entry entry collection-name)]
    (if (r/ok? res)
      (:ok res)
      (throw (ex-info "embed-for-entry failed"
                      {:result res
                       :type   (:type entry)
                       :collection collection-name})))))
