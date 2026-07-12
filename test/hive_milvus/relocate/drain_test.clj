(ns hive-milvus.relocate.drain-test
  "The relocation loop, driven entirely through its ports.

   No Milvus, no embedder, no network: the id source and the relocate function
   are both injected. The source deliberately returns rows in RANDOM order —
   that is the property the real store gives us, and the property the previous
   keyset cursor silently assumed away."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [hive-milvus.relocate :as relocate]
            [hive-milvus.relocate.source :as src])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(def ^:dynamic *cursor-base* nil)

(use-fixtures :each
  (fn [f]
    (let [dir (Files/createTempDirectory "relocate-drain-test" (make-array FileAttribute 0))]
      (try
        (binding [*cursor-base* (str dir "/cursor")]
          (f))
        (finally
          (doseq [file (reverse (file-seq (.toFile dir)))]
            (.delete file)))))))

(defrecord UnorderedSource [ids-atom]
  src/IIdSource
  (-next-ids [_ n excluded]
    (let [skip (set excluded)]
      (->> @ids-atom (remove skip) shuffle (take n) vec)))
  (-describe [_] {:source :unordered-test :remaining (count @ids-atom)}))

(defn- moving-relocate-fn
  "Models the real pipeline: a moved row LEAVES the source collection."
  [ids-atom outcome-for]
  (fn [id]
    (let [result (outcome-for id)]
      (when (:moved? result)
        (swap! ids-atom (fn [ids] (vec (remove #{id} ids)))))
      result)))

(defn- run-to-completion!
  [opts]
  (relocate/start! nil (merge {:cursor-base *cursor-base*
                              :batch-size   25
                              :concurrency  4}
                             opts))
  (let [deadline (+ (System/currentTimeMillis) 15000)]
    (loop []
      (let [s (relocate/status)]
        (cond
          (not= :running (:status s))       s
          (> (System/currentTimeMillis) deadline)
          (do (relocate/stop!)
              (throw (ex-info "drain did not terminate" {:status s})))
          :else (do (Thread/sleep 20) (recur)))))))

(defn- ids [n]
  (mapv #(format "2026071200%04d-abcdef%02d" % (mod % 100)) (range n)))

;; ============================================================================
;; The regression
;; ============================================================================

(deftest drain-moves-every-row-even-though-the-source-is-unordered
  (testing "no row is skipped when the store returns pages in arbitrary order"
    (let [all   (ids 250)
          left  (atom all)
          state (run-to-completion!
                 {:id-source   (->UnorderedSource left)
                  :relocate-fn (moving-relocate-fn left (constantly {:moved? true}))})]
      (is (= :completed (:status state)))
      (is (= 250 (:moved state)) "every row moved — the cursor version lost most of them")
      (is (empty? @left) "the source is actually drained")
      (is (zero? (:failed state)))
      (is (zero? (:skipped state))))))

(deftest counters-are-per-run-not-inherited-from-the-cursor
  (testing "a second pass over an empty source reports 0 moved, not the first pass's total"
    (let [all  (ids 40)
          left (atom all)
          _    (run-to-completion!
                {:id-source   (->UnorderedSource left)
                 :relocate-fn (moving-relocate-fn left (constantly {:moved? true}))})
          second-pass (run-to-completion!
                       {:id-source   (->UnorderedSource left)
                        :relocate-fn (moving-relocate-fn left (constantly {:moved? true}))})]
      (is (= :completed (:status second-pass)))
      (is (zero? (:moved second-pass))
          "inheriting the prior run's counters is what made an empty pass report success"))))

;; ============================================================================
;; Rows that cannot move
;; ============================================================================

(deftest unmovable-rows-are-excluded-reported-and-never-counted-as-moved
  (testing "no-ops and failures leave the source untouched, so the loop must exclude them"
    (let [all      (ids 60)
          stuck    (set (take 5 all))          ; already canonical — a no-op
          broken   (set (take 3 (drop 5 all))) ; hard failure
          left     (atom all)
          outcome  (fn [id]
                     (cond
                       (stuck id)  {:moved? false}
                       (broken id) {:moved? false :error "embed failed"}
                       :else       {:moved? true}))
          state    (run-to-completion!
                    {:id-source   (->UnorderedSource left)
                     :relocate-fn (moving-relocate-fn left outcome)})]
      (is (= :completed (:status state)) "terminates despite 8 rows it can never remove")
      (is (= 52 (:moved state)))
      (is (= 5 (:skipped state)))
      (is (= 3 (:failed state)))
      (is (= 8 (:excluded-count state)))
      (is (= 8 (count @left)) "the 8 unmovable rows are still in the source, as they should be")
      (is (= 3 (count (:failed-ids state))) "and the failures are named"))))

(deftest an-unmovable-source-stalls-instead-of-spinning-forever
  (testing "when nothing can move, stop and say stalled — never report completed"
    (let [all   (ids 100)
          left  (atom all)
          state (run-to-completion!
                 {:id-source    (->UnorderedSource left)
                  :relocate-fn  (moving-relocate-fn left (constantly {:moved? false :error "boom"}))
                  :max-excluded 20})]
      (is (= :stalled (:status state)))
      (is (not= :completed (:status state))
          "a drain that cannot drain must not claim success"))))

;; ============================================================================
;; Copy — non-destructive: the source keeps every row
;; ============================================================================

(defn- copying-relocate-fn
  "Models the copy pipeline: the row is written to the target and LEFT in the
   source. Records what was placed."
  [placed-atom]
  (fn [id]
    (swap! placed-atom conj id)
    {:moved? true :source-kept? true}))

(deftest copy-places-every-row-and-deletes-nothing
  (testing "the source still holds all its rows once the copy completes"
    (let [all    (ids 250)
          left   (atom all)               ; the source — nothing may leave it
          placed (atom #{})
          state  (run-to-completion!
                  {:id-source   (src/snapshot-source all)
                   :relocate-fn (copying-relocate-fn placed)})]
      (is (= :completed (:status state)))
      (is (= 250 (:moved state)))
      (is (= (set all) @placed) "every row was placed in the target")
      (is (= all @left) "and the source is untouched — this is the whole point"))))

(deftest a-snapshot-source-terminates-even-though-nothing-is-deleted
  (testing "the drain source would spin forever here; the snapshot cannot"
    ;; A head-reading drain relies on rows LEAVING the source. With copy they
    ;; stay, so termination has to come from exhausting a finite snapshot.
    (let [all   (ids 40)
          state (run-to-completion!
                 {:id-source   (src/snapshot-source all)
                  :relocate-fn (fn [_] {:moved? true :source-kept? true})})]
      (is (= :completed (:status state)))
      (is (= 40 (:moved state))))))

(deftest a-snapshot-hands-out-each-id-exactly-once
  (testing "no id is placed twice, even at concurrency"
    (let [all     (ids 200)
          handed  (atom [])
          _       (run-to-completion!
                   {:id-source   (src/snapshot-source all)
                    :relocate-fn (fn [id] (swap! handed conj id) {:moved? true :source-kept? true})})
          counts  (frequencies @handed)]
      (is (= 200 (count @handed)))
      (is (= #{1} (set (vals counts))) "every id handed out exactly once"))))

(deftest copy-reports-failures-without-touching-the-source
  (testing "a failed copy leaves the row where it was and is named"
    (let [all    (ids 30)
          broken (set (take 4 all))
          left   (atom all)
          state  (run-to-completion!
                  {:id-source   (src/snapshot-source all)
                   :relocate-fn (fn [id]
                                  (if (broken id)
                                    {:moved? false :error "embed failed"}
                                    {:moved? true :source-kept? true}))})]
      (is (= :completed (:status state)))
      (is (= 26 (:moved state)))
      (is (= 4 (:failed state)))
      (is (= all @left) "nothing left the source, failed or not"))))

;; ============================================================================
;; The default port
;; ============================================================================

(deftest the-no-op-source-completes-immediately
  (testing "the stub source relocates nothing and terminates"
    (let [state (run-to-completion!
                 {:id-source   (src/no-op-source)
                  :relocate-fn (fn [_] (throw (ex-info "must never be called" {})))})]
      (is (= :completed (:status state)))
      (is (zero? (:processed state))))))
