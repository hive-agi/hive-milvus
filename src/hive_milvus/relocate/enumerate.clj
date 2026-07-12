;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate.enumerate
  "Every id in a collection.

   A drain can read the head repeatedly because moved rows leave the source.
   A COPY leaves them, so it needs the full id set up front — and Milvus caps a
   query at `max-page` rows and returns them in no particular order, so a single
   page is not the collection.

   The id space is therefore split into disjoint prefix buckets, each fetched
   whole. A bucket that comes back full may have been truncated, so it is split
   again. Nothing is ever assumed about ordering, and a bucket that cannot be
   split further raises rather than returning a short answer."
  (:require [clojure.string :as str]
            [milvus-clj.api :as milvus]))

(def max-page
  "Milvus caps a query's limit here."
  16384)

(def ^:private max-depth
  "Ids carry a 14-digit timestamp prefix; past that a bucket cannot be split."
  14)

(def ^:private digits "0123456789")

(defn- succ-str
  "The next string after `s` — `s` with its last character incremented. Together
   with `s` it bounds every string having `s` as a prefix."
  [s]
  (let [n (count s)]
    (str (subs s 0 (dec n))
         (char (inc (int (.charAt ^String s (dec n))))))))

(defn- prefix-filter [prefix]
  (if (str/blank? prefix)
    "id != \"\""
    (str "id >= \"" prefix "\" and id < \"" (succ-str prefix) "\"")))

(defn- page
  [coll filt]
  (->> @(milvus/query-scalar coll
                             {:filter            filt
                              :limit             max-page
                              :output-fields     ["id"]
                              :consistency-level :strong})
       (mapv :id)))

(defn- non-digit-filter
  "Ids that do not start with a digit — the buckets below would miss them."
  []
  (str "id < \"" (first digits) "\" or id >= \"" (succ-str (str (last digits))) "\""))

(defn- ids-with-prefix
  [coll prefix depth]
  (let [rows (page coll (prefix-filter prefix))]
    (if (< (count rows) max-page)
      rows
      (if (>= depth max-depth)
        (throw (ex-info "Cannot enumerate: bucket is still full at max depth"
                        {:error  :enumerate/bucket-too-dense
                         :coll   coll
                         :prefix prefix}))
        (into (if (str/blank? prefix) (page coll (non-digit-filter)) [])
              (mapcat #(ids-with-prefix coll (str prefix %) (inc depth)))
              digits)))))

(defn all-ids
  "Every id in `coll`, as a vector. Raises rather than truncating."
  [coll]
  (vec (distinct (ids-with-prefix coll "" 0))))
