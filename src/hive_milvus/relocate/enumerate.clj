;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate.enumerate
  "Every id in a collection.

   A drain can read the head repeatedly because moved rows leave the source.
   A COPY leaves them, so it needs the full id set up front, and Milvus caps a
   query at `max-page` rows and returns them in no particular order, so a single
   page is not the collection.

   The id space is therefore split into disjoint ranges, each fetched whole. A
   range that comes back full may have been truncated, so it is split at the
   median id of what came back. Nothing is assumed about ordering or about the
   shape of ids, and a range that cannot be split further raises rather than
   returning a short answer."
  (:require [clojure.string :as str]
            [milvus-clj.api :as milvus]))

(def max-page
  "Milvus caps a query's limit here."
  16384)

(def ^:private max-depth
  "Median splits halve a full page; this many levels means the ids cannot be
   split at all (duplicates, or an ordering Milvus does not share)."
  64)

(defn- quote-id
  "`s` as a Milvus string literal."
  [s]
  (str "\"" (-> s (str/replace "\\" "\\\\") (str/replace "\"" "\\\"")) "\""))

(defn- range-filter
  "Ids in [lo, hi); a nil bound is open."
  [lo hi]
  (cond
    (and lo hi) (str "id >= " (quote-id lo) " and id < " (quote-id hi))
    lo          (str "id >= " (quote-id lo))
    hi          (str "id < " (quote-id hi))
    :else       "id != \"\""))

(defn- median-id
  "The middle id of `rows`, in string order."
  [rows]
  (let [ids (vec (sort (map :id rows)))]
    (nth ids (quot (count ids) 2))))

(defn- and-filter
  "`bucket` narrowed by the caller's `base` expression; `base` blank means none."
  [base bucket]
  (if (str/blank? base)
    bucket
    (str "(" base ") and (" bucket ")")))

(defn- too-dense [where bounds]
  (ex-info "Cannot enumerate: an id range stays full and cannot be split"
           {:error  :enumerate/bucket-too-dense
            :filter where
            :bounds bounds}))

(defn- rows-in-range
  "Lazy seq of every row matching `base` whose id lies in [lo, hi); a nil
   bound is open. FETCH is `(fn [filter-expr limit] rows)` and is never asked
   for more than `max-page`. A full page may be truncated, so the range is
   split at the page's median id: both halves exclude a row the page holds,
   so each is strictly smaller, whatever characters the ids use."
  [fetch base lo hi depth]
  (lazy-seq
   (let [rows (fetch (and-filter base (range-filter lo hi)) max-page)]
     (cond
       (< (count rows) max-page) rows
       (>= depth max-depth)      (throw (too-dense base [lo hi]))
       :else
       (let [mid (median-id rows)]
         (if (= mid lo)
           (throw (too-dense base [lo hi]))
           (concat (rows-in-range fetch base lo mid (inc depth))
                   (rows-in-range fetch base mid hi (inc depth)))))))))

(defn rows-matching
  "Up to `limit` rows satisfying `filter-expr`, read through FETCH
   (`(fn [filter-expr limit] rows)`), which is never asked for more than
   `max-page`. Within the cap this is one fetch; above it the matching rows are
   gathered from disjoint id ranges, stopping once `limit` are held.
   Raises rather than truncating."
  [fetch filter-expr limit]
  (if (<= limit max-page)
    (vec (fetch filter-expr limit))
    (into [] (take limit) (rows-in-range fetch filter-expr nil nil 0))))

(defn all-ids
  "Every id in `coll` matching the optional `base` filter expression, as a
   vector. Raises rather than truncating."
  ([coll] (all-ids coll nil))
  ([coll base]
   (let [fetch (fn [filt limit]
                 @(milvus/query-scalar coll
                                       {:filter            filt
                                        :limit             limit
                                        :output-fields     ["id"]
                                        :consistency-level :strong}))]
     (try
       (vec (distinct (map :id (rows-in-range fetch base nil nil 0))))
       (catch clojure.lang.ExceptionInfo e
         (throw (ex-info (ex-message e) (assoc (ex-data e) :coll coll) e)))))))
