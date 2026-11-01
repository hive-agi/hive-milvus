(ns hive-milvus.relocate.enumerate-test
  "Enumerating every id of a collection larger than the depth of a median split.

   The FETCH port is a stub with the behaviour measured on the live
   hive_mcp_memory_2560d (2026-10-29): a query with a limit answers the LOWEST
   `limit` matching ids, in ascending order. A median split of such a page
   advances only half a page per level, so a collection past
   `max-depth` x page/2 ids could not be enumerated. No live Milvus, no
   redefined vars: the stub is passed in."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-milvus.relocate.enumerate :as enumerate]
            [hive-milvus.store.deref :as d]))

(defn- row-id [i] (format "%08d" i))

(defn- bound [re filter-expr]
  (second (re-find re filter-expr)))

(defn- lowest-k-fetch
  "FETCH over the sorted set `ids`, answering as Milvus does: the lowest
   `limit` ids satisfying the `id >= / id > / id < ` bounds in the filter,
   ascending. Refuses a page above `enumerate/max-page`. Counts calls."
  [ids calls]
  (let [s (apply sorted-set ids)]
    (fn [filter-expr limit]
      (swap! calls inc)
      (when (> limit enumerate/max-page)
        (throw (ex-info "invalid max query result window" {:limit limit})))
      (let [ge (bound #"id >= \"([^\"]*)\"" filter-expr)
            gt (bound #"id > \"([^\"]*)\"" filter-expr)
            lt (bound #"id < \"([^\"]*)\"" filter-expr)
            xs (cond ge (subseq s >= ge)
                     gt (subseq s > gt)
                     :else (seq s))]
        (into [] (comp (take-while #(or (nil? lt) (neg? (compare % lt))))
                       (take limit)
                       (map (fn [id] {:id id})))
              xs)))))

(deftest a-collection-past-the-split-depth-is-enumerated-whole-test
  (testing "more ids than 64 median splits of a lowest-k page can reach"
    (let [n     600000
          ids   (mapv row-id (range n))
          calls (atom 0)
          rows  (enumerate/rows-matching (lowest-k-fetch ids calls)
                                         "id != \"\"" Long/MAX_VALUE)]
      (is (= n (count rows)) "every row")
      (is (= ids (mapv :id rows)) "each once, ascending")
      (is (<= @calls (inc (quot n enumerate/max-page)))
          "one fetch per page: nothing is read twice"))))

(deftest the-default-collection-size-is-enumerated-whole-test
  (testing "past the 722,882 rows of hive_mcp_memory_2560d, through all-ids-with and its count check"
    (let [n      800000
          ids    (mapv row-id (range n))
          calls  (atom 0)
          fetch  (lowest-k-fetch ids calls)
          got    (enumerate/all-ids-with (fn [filt limit] (delay (fetch filt limit)))
                                         (fn [_filt _ms] n)
                                         "coll_big" nil {})]
      (is (= n (count got)) "every id")
      (is (= ids got) "each once, ascending")
      (is (<= @calls (inc (quot n enumerate/max-page)))
          "one fetch per page: nothing is read twice"))))

(deftest a-small-page-walks-a-thousand-ids-test
  (let [ids   (mapv row-id (range 1000))
        calls (atom 0)
        rows  (enumerate/rows-matching (lowest-k-fetch ids calls)
                                       "id != \"\"" Long/MAX_VALUE {:page 4})]
    (is (= ids (mapv :id rows)))
    (is (<= @calls 251))))

(defspec every-id-once-and-at-most-one-fetch-past-the-pages 200
  (prop/for-all [n    (gen/one-of [(gen/choose 0 40) (gen/elements [0 4 8 12 16 20])])
                 page (gen/choose 1 6)]
    (let [ids   (mapv row-id (range n))
          calls (atom 0)
          got   (mapv :id (enumerate/rows-matching (lowest-k-fetch ids calls)
                                                   "id != \"\"" Long/MAX_VALUE
                                                   {:page page}))]
      (and (= ids got)
           (<= @calls (inc (quot n page)))))))

(deftest a-shuffled-page-is-still-enumerated-whole-test
  (testing "a server answering an arbitrary k in no order falls back to splitting"
    (let [ids  (mapv row-id (range 200))
          rng  (java.util.Random. 7)
          base (lowest-k-fetch ids (atom 0))
          shuf (fn [filter-expr limit]
                 (let [hits (java.util.ArrayList. ^java.util.Collection
                             (base filter-expr enumerate/max-page))]
                   (java.util.Collections/shuffle hits rng)
                   (vec (take limit hits))))
          got  (mapv :id (enumerate/rows-matching shuf "id != \"\"" Long/MAX_VALUE {:page 8}))]
      (is (= (set ids) (set got)))
      (is (= (count ids) (count got))))))

(deftest a-fetch-that-ignores-the-cursor-raises-test
  (testing "a sorted page that never moves past the cursor cannot loop forever"
    (let [stuck (fn [_ limit] (mapv (fn [i] {:id (row-id i)}) (range limit)))
          e     (try (doall (enumerate/rows-matching stuck "id != \"\"" 100 {:page 4}))
                     nil
                     (catch clojure.lang.ExceptionInfo e e))]
      (is (= :enumerate/cursor-stuck (:error (ex-data e)))))))

(deftest collect-ids-checks-the-count-test
  (let [ids (mapv row-id (range 50))]
    (testing "agreeing count: the ids come back"
      (is (= ids (enumerate/collect-ids (lowest-k-fetch ids (atom 0)) (constantly 50)
                                        nil {:page 4}))))
    (testing "a count the walk did not reach raises"
      (let [e (try (enumerate/collect-ids (lowest-k-fetch ids (atom 0)) (constantly 60)
                                          nil {:page 4})
                   nil
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (= :enumerate/count-mismatch (:error (ex-data e))))
        (is (= 50 (:read (ex-data e))))
        (is (= 60 (:count-after (ex-data e))))))
    (testing "a count that moved during the walk is accepted between the two counts"
      (let [counts   (atom [48 52])
            count-fn (fn [_] (let [c (first @counts)] (swap! counts rest) c))]
        (is (= ids (enumerate/collect-ids (lowest-k-fetch ids (atom 0)) count-fn
                                          nil {:page 4})))))))

(defn- churning-store
  "A live store over the sorted set of `ids`: FETCH answers lowest-k ascending
   (as `lowest-k-fetch`), and after the k-th page `(churn! k set)` mutates the
   set, so inserts and deletes land between pages. COUNT answers its current
   size. `stable` is filled by the caller: ids never deleted that were there
   at the start."
  [ids churn!]
  (let [s     (atom (apply sorted-set ids))
        pages (atom 0)
        fetch (fn [filter-expr limit]
                (let [ge (bound #"id >= \"([^\"]*)\"" filter-expr)
                      gt (bound #"id > \"([^\"]*)\"" filter-expr)
                      lt (bound #"id < \"([^\"]*)\"" filter-expr)
                      cur @s
                      xs (cond ge (subseq cur >= ge) gt (subseq cur > gt) :else (seq cur))
                      out (into [] (comp (take-while #(or (nil? lt) (neg? (compare % lt))))
                                         (take limit)
                                         (map (fn [id] {:id id})))
                                xs)]
                  (swap! s #(churn! (swap! pages inc) %))
                  out))]
    {:fetch fetch :count-fn (fn [_] (count @s)) :set s}))

(defn- interleaved-churn
  "Between pages k and k+1: insert 2 fresh ids just above the lowest id
   (behind the cursor from page 1 on, so never read) and delete 1 id far
   ahead of the cursor, never one of `stable`. The count rises by 1 per page
   while the walk reads 1 row fewer per page: the shape of the live refusal,
   read < min(count-before, count-after)."
  [stable]
  (fn [k s]
    (let [fresh  [(format "00000000x%04d0" k) (format "00000000x%04d1" k)]
          doomed (->> (rsubseq s > "00000000") (remove stable) (drop k) (take 1))]
      (as-> s s (apply conj s fresh) (apply disj s doomed)))))

(deftest stable-rows-survive-interleaved-inserts-and-deletes-test
  (let [n      20000
        ids    (mapv row-id (range n))
        stable (set (take-nth 2 ids))
        mk     #(churning-store ids (interleaved-churn stable))]
    (testing "the stable mode returns every row present for the whole walk, and accepts"
      (let [{:keys [fetch count-fn set]} (mk)
            got (enumerate/collect-ids fetch count-fn nil
                                       {:page 512 :consistency :snapshot-of-stable-rows})
            rep (meta got)]
        (is (every? (clojure.core/set got) stable) "every stable row")
        (is (= got (sort got)) "strictly ascending")
        (is (= (count got) (count (distinct got))))
        (is (< (count got) (min (:count-before rep) (:count-after rep)))
            "the walk read fewer rows than either count: strict would refuse this")
        (is (= :stable-rows (:complete? rep)) "never claims plain completeness")
        (is (= n (:count-before rep)))
        (is (= (count @set) (:count-after rep)))
        (is (pos? (:shortfall rep)))
        (is (<= (:shortfall rep) (:tolerance rep)))))
    (testing "the strict mode still refuses the same walk"
      (let [{:keys [fetch count-fn]} (mk)
            e (try (enumerate/collect-ids fetch count-fn nil {:page 512}) nil
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (= :enumerate/count-mismatch (:error (ex-data e))))))
    (testing "full pages that also skip stable rows under the same churn are refused"
      (let [{:keys [fetch count-fn]} (mk)
            gap    (clojure.core/set (filter stable (subvec ids 5000 5400)))
            fetch' (fn [f limit] (into [] (comp (remove #(gap (:id %))) (take limit))
                                       (fetch f (+ limit (count gap)))))
            e (try (enumerate/collect-ids fetch' count-fn nil
                                          {:page 512 :consistency :snapshot-of-stable-rows})
                   nil
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (= :enumerate/stable-rows-shortfall (:error (ex-data e))))
        (is (> (:shortfall (ex-data e)) (:tolerance (ex-data e))))))))

(deftest a-page-that-skips-stable-rows-is-refused-in-the-stable-mode-test
  (testing "full ascending pages that silently leave out 40 rows the count still holds"
    (let [ids   (mapv row-id (range 400))
          gap   (set (subvec ids 100 140))
          base  (lowest-k-fetch ids (atom 0))
          fetch (fn [f limit] (into [] (comp (remove #(gap (:id %))) (take limit))
                                    (base f (+ limit (count gap)))))
          e     (try (enumerate/collect-ids fetch (constantly 400) nil
                                            {:page 16 :consistency :snapshot-of-stable-rows})
                     nil
                     (catch clojure.lang.ExceptionInfo e e))]
      (is (= :enumerate/stable-rows-shortfall (:error (ex-data e))))
      (is (= 360 (:read (ex-data e))))
      (is (= 40 (:shortfall (ex-data e))))
      (is (= 1 (:tolerance (ex-data e))))
      (is (false? (:complete? (ex-data e))))
      (is (= 400 (:count-before (ex-data e)) (:count-after (ex-data e))))))
  (testing "the tolerance: observed churn plus a 0.1% floor, capped at 1%"
    (is (= 1 (enumerate/stable-tolerance 400 400 400)) "no churn: the floor")
    (is (= 4 (enumerate/stable-tolerance 400 420 380)) "large churn: the 1% cap")
    (is (= 5 (enumerate/stable-tolerance 1000 1002 1000)) "churn 2+2, floor 1"))
  (testing "a walk that is not strictly ascending is refused"
    (let [dup (fn [_ limit] (vec (take limit [{:id "a"} {:id "a"} {:id "b"}])))
          e   (try (enumerate/collect-ids dup (constantly 3) nil
                                          {:page 8 :consistency :snapshot-of-stable-rows})
                   nil (catch clojure.lang.ExceptionInfo e e))]
      (is (= :enumerate/duplicate-id (:error (ex-data e))))))
  (testing "an unknown consistency is refused"
    (is (thrown? clojure.lang.ExceptionInfo
                 (enumerate/collect-ids (lowest-k-fetch [] (atom 0)) (constantly 0) nil
                                        {:consistency :whatever})))))

(deftest the-stable-mode-reaches-all-ids-with-test
  (let [ids (mapv row-id (range 50))
        got (enumerate/all-ids-with (let [f (lowest-k-fetch ids (atom 0))]
                                      (fn [filt limit] (delay (f filt limit))))
                                    (fn [_ _] 50) "coll_s" nil
                                    {:page 8 :consistency :snapshot-of-stable-rows})]
    (is (= ids got))
    (is (= {:consistency :snapshot-of-stable-rows :complete? :stable-rows :coll "coll_s"
            :count-before 50 :count-after 50}
           (select-keys (meta got) [:consistency :complete? :coll :count-before :count-after])))
    (is (true? (:complete? (meta (enumerate/all-ids-with
                                  (let [f (lowest-k-fetch ids (atom 0))]
                                    (fn [filt limit] (delay (f filt limit))))
                                  (fn [_ _] 50) "coll_s" nil {:page 8}))))
        "strict stays complete")))

(deftest the-page-deadline-is-the-callers-test
  (testing "a fetch that never answers raises :milvus/timeout with the caller's bound"
    (let [e (try (enumerate/all-ids-with (fn [_filt _limit] (promise)) (fn [_filt _ms] 0)
                                         "coll_x" nil {:timeout-ms 50})
                 nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (true? (:milvus/timeout (ex-data e))))
      (is (= 50 (:timeout-ms (ex-data e))))
      (is (= "coll_x" (:coll (ex-data e))))))
  (testing "the caller's bound reaches the count too"
    (let [seen (atom nil)]
      (enumerate/all-ids-with (fn [_ _] (delay [])) (fn [_ ms] (reset! seen ms) 0)
                              "coll_x" nil {:timeout-ms 777})
      (is (= 777 @seen))))
  (testing "the default bound scales with the page"
    (is (< (d/timeout-ms) (enumerate/page-deadline-ms enumerate/max-page))))
  (testing "with no :timeout-ms, an enumeration page and its count wait at least 30 s"
    (let [seen (atom [])]
      (enumerate/all-ids-with (fn [_ _] (delay [])) (fn [_ ms] (swap! seen conj ms) 0)
                              "coll_x" nil {})
      (is (seq @seen))
      (is (every? #(>= % 30000) @seen)))
    (is (>= (enumerate/page-deadline-ms enumerate/max-page) 30000)
        "a 16384-row id page measured ~2 s idle; under load it needs far more than 5 s")))

(deftest a-snapshot-source-passes-the-callers-options-test
  (testing "milvus-snapshot-source hands its options through to all-ids"
    (let [seen (atom nil)]
      (with-redefs [enumerate/all-ids (fn [coll base opts] (reset! seen [coll base opts]) ["a"])]
        ((requiring-resolve 'hive-milvus.relocate.source/milvus-snapshot-source)
         "coll_y" {:timeout-ms 45000}))
      (is (= ["coll_y" nil {:timeout-ms 45000}] @seen)))))
