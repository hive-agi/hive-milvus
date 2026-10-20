;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.store.lookup
  "Leaf ns for cross-collection id lookup. Extracted out of
   `hive-milvus.store.entries` so the `relocate/` pipeline can call
   into it without creating a cycle (entries → pipeline → collectors
   → entries). No upward dependencies — only `routing` + `query`."
  (:require [hive-milvus.store.query :as query]
            [hive-milvus.store.routing :as routing]
            [malli.core :as m]
            [hive-milvus.embed.port :as port]
            [hive-dsl.result :as r]))

(defn legacy-coll-name
  "The collection pinned by `:collection-name` in config, or nil when the
   user has not pinned one."
  [config-atom]
  (:collection-name @config-atom))

(defn- searchable?
  "True when a configured provider can embed a query into `coll`'s space.
   Config-only — the store preloads its collections before the embedding
   registry exists, so this must not build a provider."
  [coll]
  (port/collection-backed? coll))

(defn known-collections
  "Every collection a type-less read may fan out over: the collections backing
   the configured embedding providers, plus a pinned `:collection-name` when a
   provider still backs it. A collection whose provider is no longer configured
   is not searched."
  [config-atom]
  (->> (cons (legacy-coll-name config-atom) (routing/all-known-collections))
       (remove nil?)
       distinct
       (filterv searchable?)))

(def ^:private missing-collection-markers
  "Message fragments Milvus (gRPC status 100 / REST code 100, across 2.4-2.6)
   uses for a collection that does not exist."
  [#"(?i)collection not found"
   #"(?i)can't find collection"
   #"(?i)collection not exist"
   #"(?i)collection .* does not exist"])

(defn missing-collection?
  "True when `t`, or any link of its cause chain, says the collection does
   not exist. A configured collection that was never created (e.g.
   `hive_mcp_memory_1024d` before its first routed write, or after a failed
   preload) holds nothing, so asking it is a definite ANSWER - absent - not
   an outage. Pure."
  [^Throwable t]
  (boolean
   (some (fn [^Throwable link]
           (when-let [msg (.getMessage link)]
             (some #(re-find % msg) missing-collection-markers)))
         (take 10 (take-while some? (iterate #(.getCause ^Throwable %) t))))))

(defn find-entry-collection
  "Locate which known collection holds `id`. Returns the collection
   name or nil. Each lookup is wrapped in try so a missing collection
   (e.g. `hive_mcp_memory_1024d` before the first qwen3-routed write,
   see `missing-collection?`) doesn't abort the scan.

   NOTE: this still swallows EVERY per-collection exception, so an outage
   reads as nil here; `store.entries/locate-entry` is the outcome-honest
   fold."
  [config-atom id]
  (some (fn [coll]
          (try
            (when (query/get-entry-by-id coll id) coll)
            (catch Exception _ nil)))
        (known-collections config-atom)))

(defn locate-collection
  "Outcome-honest twin of `find-entry-collection`: ask each of `colls`, in
   order, for `id` through FETCH (`(fn [coll id] value-or-nil)`), stopping
   at the first value.

     r/ok {:collection c :value v}  collection `c` answered with `v`
     r/ok nil                       EVERY collection answered and none holds
                                    `id` - a true absence. A collection that
                                    does not exist (`missing-collection?`)
                                    counts as answered: it holds nothing.
     r/err :milvus/read-incomplete {:id :failed [{:collection :message}]}
                                    no value found AND at least one
                                    collection failed, so absence cannot be
                                    told from an outage

   A hit wins over a failing neighbour. Never throws. Pure given FETCH."
  [fetch colls id]
  (let [{:keys [found failed]}
        (reduce (fn [acc coll]
                  (try
                    (if-some [v (fetch coll id)]
                      (reduced (assoc acc :found {:collection coll :value v}))
                      acc)
                    (catch Exception e
                      (if (missing-collection? e)
                        acc
                        (update acc :failed conj
                                {:collection coll
                                 :message    (or (ex-message e) (str (class e)))})))))
                {:failed []}
                colls)]
    (cond
      (some? found) (r/ok found)
      (seq failed)  (r/err :milvus/read-incomplete {:id id :failed failed})
      :else         (r/ok nil))))

(m/=> known-collections
      [:=> [:cat [:fn {:error/message "config atom"} #(instance? clojure.lang.IAtom %)]]
       [:vector :string]])