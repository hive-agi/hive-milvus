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

(defn- non-digit-filter
  "Ids that do not start with a digit — the buckets below would miss them."
  []
  (str "id < \"" (first digits) "\" or id >= \"" (succ-str (str (last digits))) "\""))

(defn- and-filter
  "`bucket` narrowed by the caller's `base` expression; `base` blank means none."
  [base bucket]
  (if (str/blank? base)
    bucket
    (str "(" base ") and (" bucket ")")))

(defn- too-dense [where prefix]
  (ex-info "Cannot enumerate: bucket is still full at max depth"
           {:error  :enumerate/bucket-too-dense
            :filter where
            :prefix prefix}))

(defn- rows-with-prefix
  "Lazy seq of every row matching `base` whose id starts with `prefix`. FETCH is
   `(fn [filter-expr limit] rows)` and is never asked for more than `max-page`."
  [fetch base prefix depth]
  (lazy-seq
   (let [rows (fetch (and-filter base (prefix-filter prefix)) max-page)]
     (cond
       (< (count rows) max-page) rows
       (>= depth max-depth)      (throw (too-dense base prefix))
       :else
       (concat
        (when (str/blank? prefix)
          (let [odd (fetch (and-filter base (non-digit-filter)) max-page)]
            (when (>= (count odd) max-page)
              (throw (too-dense base :non-digit)))
            odd))
        (mapcat #(rows-with-prefix fetch base (str prefix %) (inc depth))
                digits))))))

(defn rows-matching
  "Up to `limit` rows satisfying `filter-expr`, read through FETCH
   (`(fn [filter-expr limit] rows)`), which is never asked for more than
   `max-page`. Within the cap this is one fetch; above it the matching rows are
   gathered from disjoint id-prefix buckets, stopping once `limit` are held.
   Raises rather than truncating."
  [fetch filter-expr limit]
  (if (<= limit max-page)
    (vec (fetch filter-expr limit))
    (into [] (take limit) (rows-with-prefix fetch filter-expr "" 0))))

(defn all-ids
  "Every id in `coll`, as a vector. Raises rather than truncating."
  [coll]
  (let [fetch (fn [filt limit]
                @(milvus/query-scalar coll
                                      {:filter            filt
                                       :limit             limit
                                       :output-fields     ["id"]
                                       :consistency-level :strong}))]
    (try
      (vec (distinct (map :id (rows-with-prefix fetch nil "" 0))))
      (catch clojure.lang.ExceptionInfo e
        (throw (ex-info (ex-message e) (assoc (ex-data e) :coll coll) e))))))
