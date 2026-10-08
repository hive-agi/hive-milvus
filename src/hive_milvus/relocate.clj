;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate
  "Background re-embedding + relocation of memory entries from
   non-canonical Milvus collections (e.g. legacy 768-d
   `hive_mcp_memory`) into per-dim collections (`hive_mcp_memory_1024d`,
   `hive_mcp_memory_4096d`) driven by current type-routing config.

   Idempotent + resumable. `entries/relocate-entry!` is itself a no-op
   once an entry is already in its canonical collection, so resume
   never duplicates."
  (:require [clojure.java.io :as io]
            [hive-milvus.cursor :as cursor]
            [hive-milvus.store.entries :as entries]
            [taoensso.timbre :as log]
            [hive-milvus.relocate.plan :as plan]
            [hive-milvus.relocate.source :as src]
            [hive-dsl.result :as r]
            [hive-milvus.relocate.pipeline :as pipeline]))

(def ^:private state
  (atom {:job-id        nil
         :status        :idle
         :source-coll   nil
         :started-at    nil
         :stopping?     false
         :processed     0
         :moved         0
         :skipped       0
         :failed        0
         :failed-ids    []
         :skipped-ids   []
         :excluded-ids  #{}
         :last-id       nil
         :last-batch-at nil
         :batch-size    100
         :max-excluded  nil
         :cursor-path   nil
         :error-message nil}))

(def ^:private default-cursor-base
  (str (System/getProperty "user.home")
       "/.local/share/hive-mcp/relocate-cursor"))

(def ^:private default-batch-size 100)

(def ^:private failed-ids-cap 50)

(def ^:private default-source-collection "hive_mcp_memory")

(defn status
  "Return current relocation state snapshot + on-disk cursor."
  []
  (let [s @state
        c (when (:cursor-path s)
            (cursor/read-cursor (:cursor-path s)))]
    (-> s
        (dissoc :cursor-path :excluded-ids)
        (assoc :excluded-count (count (:excluded-ids s)))
        (assoc :cursor c))))

(defn stop!
  "Request the running relocation to stop after the current batch."
  []
  (if (= :running (:status @state))
    (do (swap! state assoc :stopping? true)
        {:requested-stop? true :was :running})
    {:requested-stop? false :was (:status @state)}))

(defn- capped-conj
  [xs id]
  (cond-> xs (< (count xs) failed-ids-cap) (conj id)))

(defn- record-result!
  [acc id result]
  (cond-> (-> acc (update :processed inc) (assoc :last-id id))
    (:moved? result)
    (update :moved inc)

    (and (false? (:moved? result)) (not (:error result)))
    (-> (update :skipped inc)
        (update :excluded-ids conj id)
        (update :skipped-ids capped-conj id))

    (:error result)
    (-> (update :failed inc)
        (update :excluded-ids conj id)
        (update :failed-ids capped-conj id))))

(defn- run-concurrently
  "Apply `f` to each of `ids` with bounded parallelism (per-state
   :concurrency, default 1). Returns [id outcome] pairs in order; an id whose
   task saw `:stopping?` before it started pairs with ::stopping. A thrown
   exception is folded into `{:moved? false :error msg :id id}`."
  [f ids]
  (let [conc     (max 1 (or (:concurrency @state) 1))
        executor (java.util.concurrent.Executors/newFixedThreadPool conc)]
    (try
      (let [futures (mapv
                      (fn [id]
                        (.submit ^java.util.concurrent.ExecutorService executor
                                 ^Callable
                                 (fn []
                                   (if (:stopping? @state)
                                     [id ::stopping]
                                     [id (try (f id)
                                              (catch Throwable e
                                                {:moved? false :error (.getMessage e) :id id}))]))))
                      ids)]
        (mapv (fn [^java.util.concurrent.Future fut] (.get fut)) futures))
      (finally
        (.shutdown ^java.util.concurrent.ExecutorService executor)))))

(defn- fold-pairs
  "Fold [id outcome] pairs into a round summary, skipping ::stopping."
  [pairs]
  (reduce
    (fn [acc [id result]]
      (if (= ::stopping result)
        acc
        (record-result! acc id result)))
    {:processed 0 :moved 0 :skipped 0 :failed 0
     :failed-ids [] :skipped-ids [] :excluded-ids #{}
     :last-id   nil}
    pairs))

(defn- run-batch!
  "Process `ids` concurrently with bounded parallelism (per-state
   :concurrency, default 1), folding each outcome into a round summary.

   `relocate-fn` takes an id and returns {:moved? bool} or {:error msg}.

   `:stopping?` is checked before each task starts; in-flight tasks
   complete (no mid-flight cancel of Milvus/Venice calls) so we may
   overshoot by up to (concurrency-1) entries before halting."
  [relocate-fn ids]
  (fold-pairs (run-concurrently relocate-fn ids)))

(defn- run-page!
  "Batched twin of `run-batch!` for the pipeline-backed modes: prepare every
   id concurrently (collect, route, embed), then write the whole page with ONE
   upsert per target collection (`pipeline/commit-page`). `unwrap` turns each
   id's Result into the raw {:moved? ..} / {:error ..} shape the runner folds.
   A failed batch write falls back to per-record writes, so every failed id
   is still named."
  [{:keys [prepare-fn writer disposition unwrap]} ids]
  (let [pairs    (run-concurrently prepare-fn ids)
        stopped  (filterv (fn [[_ res]] (= ::stopping res)) pairs)
        ;; a thrown prepare is folded by run-concurrently into
        ;; {:moved? false :error msg}, which is not r/ok and passes through
        prepared (filterv (fn [[_ res]] (not= ::stopping res)) pairs)
        placed   (pipeline/commit-page writer disposition prepared)]
    (fold-pairs (into (mapv (fn [[id res]] [id (unwrap id res)]) placed)
                      stopped))))

(defn- merge-batch-into-state!
  [batch-result]
  (swap! state
         (fn [s]
           (-> s
               (update :processed + (:processed batch-result))
               (update :moved     + (:moved     batch-result))
               (update :skipped   + (:skipped   batch-result))
               (update :failed    + (:failed    batch-result))
               (update :excluded-ids into (:excluded-ids batch-result))
               (update :failed-ids
                       (fn [xs]
                         (->> (concat xs (:failed-ids batch-result))
                              (take failed-ids-cap)
                              vec)))
               (update :skipped-ids
                       (fn [xs]
                         (->> (concat xs (:skipped-ids batch-result))
                              (take failed-ids-cap)
                              vec)))
               (cond-> (:last-id batch-result)
                       (assoc :last-id (:last-id batch-result)))
               (assoc :last-batch-at (cursor/now-iso))))))

(defn- checkpoint-cursor!
  [cursor-path]
  (cursor/write-cursor! cursor-path (-> @state (dissoc :cursor-path))))

(defn- finalize!
  [cursor-path final-status]
  (swap! state assoc
         :status        final-status
         :last-batch-at (cursor/now-iso)
         :stopping?     false)
  (checkpoint-cursor! cursor-path))

(defn- runner-loop!
  [{:keys [source run-page cursor-path batch-size max-excluded]}]
  (try
    (loop []
      (let [{:keys [stopping? excluded-ids]} @state
            over?  (> (count excluded-ids) max-excluded)
            ids    (if (or stopping? over?)
                     []
                     (src/-next-ids source batch-size excluded-ids))
            batch  (when (seq ids) (run-page ids))
            _      (when batch
                     (merge-batch-into-state! batch)
                     (checkpoint-cursor! cursor-path)
                     (log/info "relocate batch done"
                               {:source  (src/-describe source)
                                :page    (count ids)
                                :moved   (:moved batch)
                                :skipped (:skipped batch)
                                :failed  (:failed batch)}))
            v      (plan/verdict {:page-count     (count ids)
                                  :classified     (if batch (:processed batch) 0)
                                  :excluded-count (count excluded-ids)
                                  :max-excluded   max-excluded
                                  :stopping?      (boolean stopping?)})]
        (if (= :continue v)
          (recur)
          (finalize! cursor-path v))))
    (catch Throwable e
      (log/error e "relocate runner failed")
      (swap! state assoc
             :status        :failed
             :error-message (.getMessage e)
             :last-batch-at (cursor/now-iso))
      (checkpoint-cursor! cursor-path))))

(defn- copy-entry!
  "Copy `id` into its canonical collection, leaving the source row in place.
   Unwraps the pipeline Result into the raw shape the runner folds."
  [config-atom id]
  (let [res (pipeline/copy-one config-atom id)]
    (if (r/ok? res)
      (:ok res)
      {:moved? false
       :error  (or (:error res) :copy/failed)
       :id     id})))

(defn unwrap-placed
  "Pure: the raw shape the runner folds, from a `commit-page` Result for `id`.
   r/ok -> the place-one answer with :moved? mirroring :placed?. An outcome
   that is already raw (a prepare that threw) is kept. A :collector/not-found
   err is a skip (`entries/relocate-entry!` answers it the same way); any
   other err is a named failure."
  [id res]
  (cond
    (r/ok? res)
    (let [v (:ok res)] (assoc v :moved? (:placed? v)))

    (contains? res :moved?)
    res

    (= :collector/not-found (:error res))
    {:moved? false :from nil :to nil :id id :reason :not-found}

    :else
    {:moved? false :error (or (:error res) :relocate/failed) :id id}))

(defn- page-runner
  "ids -> round summary. The pipeline modes batch their writes (`run-page!`):
   one upsert per target collection per page. An injected `relocate-fn`, or
   `batch-writes?` false, keeps the per-id path (`run-batch!`)."
  [config-atom mode relocate-fn batch-writes?]
  (cond
    relocate-fn
    #(run-batch! relocate-fn %)

    (not batch-writes?)
    #(run-batch! (case mode
                   :copy (fn [id] (copy-entry! config-atom id))
                   :move (fn [id] (entries/relocate-entry! config-atom id)))
                 %)

    :else
    #(run-page! {:prepare-fn  (fn [id] (pipeline/prepare-one config-atom id))
                 :writer      (pipeline/milvus-writer)
                 :disposition (case mode
                                :copy (pipeline/keep-source)
                                :move (pipeline/delete-source))
                 :unwrap      unwrap-placed}
                %)))

(defn start!
  "Spawn a background relocation pass. Returns immediately with the new
   job's metadata, or {:already-running? true} when a previous job is
   still :running.

   :mode :move (default) removes each row from the source once it has landed in
   the target, and iterates by re-reading the head of the source.

   :mode :copy leaves every source row where it is. The old collection stays a
   complete backup. Because nothing is removed, the head would never empty, so
   the pass enumerates the source's ids up front and works through that snapshot.

   Counters are per-run. Rows the pass cannot place (already-canonical no-ops,
   hard failures) are excluded from subsequent pages so the run terminates; they
   are reported, never silently counted as placed.

   Writes are batched: each page is prepared concurrently, then written with
   ONE upsert per target collection (`pipeline/commit-page`). A failed batch
   falls back to per-record writes so every failed id is still named.

   Opts: :mode :source-coll :batch-size :cursor-base :concurrency :max-excluded
         :id-source   (IIdSource, overrides the mode's default source)
         :relocate-fn (id -> result, overrides the mode's default operation)
         :batch-writes? (default true; false writes one record per round-trip)"
  ([config-atom]
   (start! config-atom {}))
  ([config-atom {:keys [mode source-coll batch-size cursor-base concurrency
                        id-source relocate-fn max-excluded batch-writes?]
                 :or   {mode          :move
                        source-coll   default-source-collection
                        batch-size    default-batch-size
                        cursor-base   default-cursor-base
                        concurrency   1
                        max-excluded  plan/default-max-excluded
                        batch-writes? true}}]
   (if (= :running (:status @state))
     {:already-running? true :status (status)}
     (let [job-id      (str "reloc-" (System/currentTimeMillis))
           cursor-path (cursor/cursor-path-for cursor-base source-coll)
           _           (io/make-parents cursor-path)
           source      (or id-source
                           (case mode
                             :copy (src/milvus-snapshot-source source-coll)
                             :move (src/milvus-drain-source source-coll)))
           run-page    (page-runner config-atom mode relocate-fn batch-writes?)]
       (reset! state {:job-id        job-id
                      :status        :running
                      :mode          mode
                      :source-coll   source-coll
                      :started-at    (cursor/now-iso)
                      :stopping?     false
                      :processed     0
                      :moved         0
                      :skipped       0
                      :failed        0
                      :failed-ids    []
                      :skipped-ids   []
                      :excluded-ids  #{}
                      :last-id       nil
                      :last-batch-at nil
                      :batch-size    batch-size
                      :concurrency   concurrency
                      :max-excluded  max-excluded
                      :cursor-path   cursor-path
                      :error-message nil})
       (future (runner-loop! {:source       source
                              :run-page     run-page
                              :cursor-path  cursor-path
                              :batch-size   batch-size
                              :max-excluded max-excluded}))
       {:job-id       job-id
        :mode         mode
        :source-coll  source-coll
        :cursor-path  cursor-path
        :source       (src/-describe source)
        :batch-size   batch-size
        :concurrency  concurrency
        :max-excluded max-excluded}))))

(defn reset-cursor!
  "Delete the on-disk cursor for a source collection."
  ([] (reset-cursor! default-source-collection default-cursor-base))
  ([source-coll]
   (reset-cursor! source-coll default-cursor-base))
  ([source-coll cursor-base]
   (let [path (cursor/cursor-path-for cursor-base source-coll)
         f    (io/file path)]
     (if (.exists f)
       (do (.delete f) {:deleted? true :path path})
       {:deleted? false :path path :reason :not-found}))))
