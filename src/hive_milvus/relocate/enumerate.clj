;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate.enumerate
  "Every id in a collection.

   A drain can read the head repeatedly because moved rows leave the source.
   A COPY leaves them, so it needs the full id set up front, and Milvus caps a
   query at `max-page` rows, so a single page is not the collection.

   Milvus answers a query with a limit with the LOWEST matching primary keys,
   in ascending order (measured on hive_mcp_memory_2560d, 2026-10-29; the Java
   SDK's QueryIterator relies on the same contract). The walk is therefore
   keyset paging: a full page that comes back ascending moves a cursor to its
   last id and the next page asks for `id > cursor`. Every page strictly
   advances the cursor, so the walk ends with no depth bound and reads each
   row once.

   A full page that is NOT ascending means the server does not keep that
   contract; that range is then split at the page's median id, both halves
   strictly smaller. A cursor that does not advance, or a range still full
   after `max-depth` splits, raises rather than returning a short answer, and
   `collect-ids` checks the walk against the server's count(*)."
  (:require [hive-milvus.resilience.budget :as budget]
            [hive-milvus.store.deref :as d]
            [clojure.string :as str]
            [milvus-clj.api :as milvus]))

(def max-page
  "Milvus caps a query's limit here."
  16384)

(def ^:private max-depth
  "Median splits (the fallback for a page that is not ascending) halve a full
   page; this many levels means the ids cannot be split at all."
  64)

(defn- quote-id
  "`s` as a Milvus string literal."
  [s]
  (str "\"" (-> s (str/replace "\\" "\\\\") (str/replace "\"" "\\\"")) "\""))

(defn- range-filter
  "Ids above `lo` and below `hi`. `lo` is nil (open) or `{:id s :after? b}`,
   `:after?` excluding `s` itself (a cursor); `hi` is exclusive, nil open."
  [lo hi]
  (let [clauses (cond-> []
                  lo (conj (str "id " (if (:after? lo) ">" ">=") " " (quote-id (:id lo))))
                  hi (conj (str "id < " (quote-id hi))))]
    (if (seq clauses)
      (str/join " and " clauses)
      "id != \"\"")))

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

(defn- cursor-stuck [where bounds last-id]
  (ex-info "Cannot enumerate: a full ascending page did not move past its cursor"
           {:error   :enumerate/cursor-stuck
            :filter  where
            :bounds  bounds
            :last-id last-id}))

(defn- ascending?
  "`ids` in non-decreasing string order."
  [ids]
  (loop [prev nil xs (seq ids)]
    (cond
      (nil? xs)                                     true
      (and prev (pos? (compare prev (first xs))))   false
      :else                                         (recur (first xs) (next xs)))))

(defn- page-size
  "A caller's page, within Milvus's cap."
  [page]
  (min max-page (max 1 (or page max-page))))

(defn- rows-in-range
  "Lazy seq of every row matching `base` whose id lies above `lo` and below
   `hi` (see `range-filter`). FETCH is `(fn [filter-expr limit] rows)` and is
   asked for `page` rows at a time.

   A short page is the whole range. A full ascending page holds the lowest ids
   of the range, so the rest lies strictly after its last id: the walk goes on
   from there (keyset paging), one fetch per page, no depth bound. A full page
   in any other order may be an arbitrary subset, so the range is split at the
   page's median id instead: both halves exclude a row the page holds, so each
   is strictly smaller."
  [fetch base page lo hi depth]
  (lazy-seq
   (let [rows (vec (fetch (and-filter base (range-filter lo hi)) page))
         ids  (mapv :id rows)]
     (cond
       (< (count rows) page) rows

       (ascending? ids)
       (let [last-id (peek ids)]
         (when (and lo (not (pos? (compare last-id (:id lo)))))
           (throw (cursor-stuck base [lo hi] last-id)))
         (concat rows (rows-in-range fetch base page {:id last-id :after? true} hi depth)))

       (>= depth max-depth) (throw (too-dense base [lo hi]))

       :else
       (let [mid (median-id rows)]
         (if (= mid (:id lo))
           (throw (too-dense base [lo hi]))
           (concat (rows-in-range fetch base page lo mid (inc depth))
                   (rows-in-range fetch base page {:id mid :after? false} hi (inc depth)))))))))

(defn rows-matching
  "Up to `limit` rows satisfying `filter-expr`, read through FETCH
   (`(fn [filter-expr limit] rows)`), which is never asked for more than
   `:page` rows (default and ceiling `max-page`). Within a page this is one
   fetch; above it the rows are walked in ascending id order, stopping once
   `limit` are held. Raises rather than truncating."
  ([fetch filter-expr limit] (rows-matching fetch filter-expr limit {}))
  ([fetch filter-expr limit {:keys [page]}]
   (let [page (page-size page)]
     (if (<= limit page)
       (vec (fetch filter-expr limit))
       (into [] (take limit) (rows-in-range fetch filter-expr page nil nil 0))))))

(def enumerate-timeout-ms
  "Floor of the default deref bound for one enumeration call. A full id page
   measured ~2 s on an idle hive_mcp_memory_2560d; a walk of ~45 such pages
   runs while the store serves traffic, so the floor is the read cap, not the
   5 s interactive default."
  budget/read-deadline-cap-ms)

(defn page-deadline-ms
  "Default deref bound for an id-only page of `rows` rows: the base deadline
   plus a per-row allowance (a 16384-row strong read measured ~2 s), never
   below `enumerate-timeout-ms`."
  [rows]
  (max enumerate-timeout-ms
       (budget/read-deadline-ms (d/timeout-ms) rows budget/projected-row-cost-ms)))

(def consistency-modes
  "What `collect-ids` holds a walk to.

   :strict (default): the walk must account for the server's count(*). With
   an unchanged count it read exactly that many rows; with a count that moved
   during the walk (live writes) the rows read lie between the two counts.

   :snapshot-of-stable-rows: the guarantee asked for is that every row present
   for the WHOLE walk is returned. Rows inserted or deleted during the walk may
   or may not appear. See `collect-ids` for the acceptance rule."
  #{:strict :snapshot-of-stable-rows})

(def stable-tolerance-floor
  "Under :snapshot-of-stable-rows, the part of the shortfall tolerance that does
   not depend on churn: this fraction of min(count-before, count-after). It
   covers churn that the counts cannot see, such as an insert behind the
   cursor offset by a delete ahead of it."
  1/1000)

(def stable-tolerance-cap
  "Under :snapshot-of-stable-rows, the tolerance never exceeds this fraction of
   min(count-before, count-after), however much churn was observed. A walk
   short by more than that is refused."
  1/100)

(defn stable-tolerance
  "Rows the walk may fall short of min(`before`, `after`) under
   :snapshot-of-stable-rows: the churn the three counts show, |mid - before| +
   |after - mid|, plus ceil(`stable-tolerance-floor` x min). The whole is
   capped at ceil(`stable-tolerance-cap` x min)."
  [before mid after]
  (let [base  (min before after)
        churn (+ (abs (- mid before)) (abs (- after mid)))
        floor (long (Math/ceil (* base stable-tolerance-floor)))
        cap   (long (Math/ceil (* base stable-tolerance-cap)))]
    (min cap (+ churn floor))))

(defn- walk-ascending!
  "The ids of `rows`, realized in walk order. Raises
   :enumerate/duplicate-id or :enumerate/not-ascending unless each id is
   strictly greater than the one before it. Calls `on-mid` once, as the walk
   reaches row `mid-at`."
  [rows mid-at on-mid base]
  (loop [xs (seq rows) prev nil i 0 acc (transient [])]
    (when (= i mid-at) (on-mid))
    (if xs
      (let [id (:id (first xs))
            c  (when prev (compare id prev))]
        (when (and c (not (pos? c)))
          (throw (ex-info (str "Cannot enumerate: id " (pr-str id) " at row " i
                               (if (zero? c) " repeats the one before it"
                                   " is below the one before it"))
                          {:error  (if (zero? c) :enumerate/duplicate-id :enumerate/not-ascending)
                           :at     i
                           :id     id
                           :prev   prev
                           :filter base})))
        (recur (next xs) id (inc i) (conj! acc id)))
      (persistent! acc))))

(defn- collect-stable
  "`collect-ids` under :snapshot-of-stable-rows."
  [fetch count-fn base filt page]
  (let [before (count-fn filt)
        mid    (volatile! nil)
        ids    (walk-ascending! (rows-in-range fetch base page nil nil 0)
                                (when (pos? before) (quot before 2))
                                #(vreset! mid (count-fn filt))
                                base)
        mid    (or @mid (count-fn filt))
        after  (count-fn filt)
        n      (count ids)
        tol    (stable-tolerance before mid after)
        short  (max 0 (- (min before after) n))
        report {:consistency  :snapshot-of-stable-rows
                :complete?    :stable-rows
                :read         n
                :count-before before
                :count-mid    mid
                :count-after  after
                :tolerance    tol
                :shortfall    short}]
    (when (> short tol)
      (throw (ex-info (str "Cannot enumerate: the walk read " n " rows, " short
                           " short of min(count(*)) (" before " before, " mid " mid-walk, "
                           after " after); churn explains at most " tol)
                      (assoc report
                             :error     :enumerate/stable-rows-shortfall
                             :complete? false
                             :filter    base))))
    (with-meta ids report)))

(defn collect-ids
  "Every id matching `base`, read through FETCH (`(fn [filter-expr limit]
   rows)`) and checked against COUNT-FN (`(fn [filter-expr] n)`, the server's
   count(*)). Returns a vector of distinct ids, in walk order. Its metadata is
   the walk's report: :consistency, :complete?, :read, :count-before and
   :count-after, plus :count-mid, :tolerance and :shortfall in the stable
   mode. Options: `:page` as in `rows-matching`, and `:consistency` (one of
   `consistency-modes`, default :strict).

   :strict takes a count before and after the walk. If the count did not
   change, the walk must have read exactly that many rows. If it moved
   (live writes), the rows read must lie between the two counts. Anything
   else raises :enumerate/count-mismatch. :complete? is true.

   :snapshot-of-stable-rows takes counts before, mid-walk (at row
   before/2) and after the walk. It accepts the walk only when BOTH hold:
     1. Every id is strictly greater than the one before it. Otherwise it
        raises :enumerate/duplicate-id or :enumerate/not-ascending. This is
        the keyset proof. Each page is the lowest ids above the cursor, so a
        row present for the whole walk lies above some cursor and below the
        next page's last id, and is returned. Primary keys are immutable, so
        no row moves behind the cursor. A server that answers in another
        order (the median-split fallback) gives no such proof and is
        refused.
     2. read >= min(before, after) - tolerance, where tolerance is
        `stable-tolerance`: the churn the three counts show, plus 0.1% of
        min(before, after), capped at 1% of it. Otherwise it raises
        :enumerate/stable-rows-shortfall.
   Rule 2 is a gap detector, not a proof. Counts show only net churn, so an
   insert behind the cursor and a delete ahead of it cancel out. That is why
   the tolerance has a floor. A page that drops more stable rows than the
   tolerance is refused, and a smaller drop cannot be told apart from churn.
   The report carries every count, the tolerance and the shortfall, and
   :complete? is :stable-rows (never true), so the caller sees what was
   accepted."
  [fetch count-fn base {:keys [page consistency] :or {consistency :strict}}]
  (when-not (consistency-modes consistency)
    (throw (ex-info (str "Unknown enumeration consistency " (pr-str consistency))
                    {:error :enumerate/unknown-consistency
                     :consistency consistency
                     :known consistency-modes})))
  (let [filt (if (str/blank? base) "id != \"\"" base)]
    (if (= consistency :snapshot-of-stable-rows)
      (collect-stable fetch count-fn base filt (page-size page))
      (let [before (count-fn filt)
            ids    (mapv :id (rows-in-range fetch base (page-size page) nil nil 0))
            after  (count-fn filt)
            n      (count ids)]
        (when-not (<= (min before after) n (max before after))
          (throw (ex-info (str "Cannot enumerate: the walk read " n " rows but count(*) said "
                               before " before and " after " after")
                          {:error        :enumerate/count-mismatch
                           :read         n
                           :count-before before
                           :count-after  after
                           :filter       base})))
        (with-meta (vec (distinct ids))
          {:consistency  :strict
           :complete?    true
           :read         n
           :count-before before
           :count-after  after})))))

(defn all-ids-with
  "`collect-ids` over QUERY (`(fn [filter-expr limit] future-of-rows)`) and
   COUNT (`(fn [filter-expr timeout-ms] n)`). Each page is dereferenced within
   `:timeout-ms` when given, else `page-deadline-ms` of its size; a count
   within `:timeout-ms`, else `budget/read-deadline-cap-ms`. `:consistency`
   and `:page` go to `collect-ids`. A failure carries `:coll`, and so does
   the report in the result's metadata."
  [query count-q coll base {:keys [timeout-ms page consistency]}]
  (let [fetch (fn [filt limit]
                (d/deref! :query-scalar (query filt limit)
                          (or timeout-ms (page-deadline-ms limit))))
        cnt   (fn [filt] (count-q filt (or timeout-ms budget/read-deadline-cap-ms)))]
    (try
      (let [ids (collect-ids fetch cnt base (cond-> {:page page}
                                              consistency (assoc :consistency consistency)))]
        (vary-meta ids assoc :coll coll))
      (catch clojure.lang.ExceptionInfo e
        (throw (ex-info (ex-message e) (assoc (ex-data e) :coll coll) e))))))

(defn- count-matching
  "Server-side count(*) of the rows of `coll` matching `filt`."
  [coll filt ms]
  (let [row (first (d/deref! :count (milvus/query-scalar coll
                                      {:filter            filt
                                       :output-fields     ["count(*)"]
                                       :limit             1
                                       :consistency-level :strong})
                             ms))
        n   (get row (keyword "count(*)"))]
    (if (nat-int? n)
      n
      (throw (ex-info "Milvus count aggregation returned no count"
                      {:error :milvus/count-unavailable :coll coll :row row})))))

(defn all-ids
  "Every id in `coll` matching the optional `base` filter expression, as a
   vector, checked against count(*). Options: `:timeout-ms` bounds each Milvus
   call (default: scaled to the page), `:page` is the page size, and
   `:consistency` is :strict (default) or :snapshot-of-stable-rows (see
   `collect-ids`). The walk's report is the result's metadata. Raises rather
   than truncating."
  ([coll] (all-ids coll nil))
  ([coll base] (all-ids coll base {}))
  ([coll base opts]
   (all-ids-with (fn [filt limit]
                   (milvus/query-scalar coll
                                        {:filter            filt
                                         :limit             limit
                                         :output-fields     ["id"]
                                         :consistency-level :strong}))
                 (fn [filt ms] (count-matching coll filt ms))
                 coll base opts)))
