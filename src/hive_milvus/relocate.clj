;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.relocate
  "Background re-embedding + relocation of memory entries from
   non-canonical Milvus collections (e.g. legacy 768-d
   `hive_mcp_memory`) into per-dim collections (`hive_mcp_memory_1024d`,
   `hive_mcp_memory_4096d`) driven by current type-routing config.

   Idempotent + resumable. `entries/relocate-entry!` is itself a no-op
   once an entry is already in its canonical collection, so resume
   never duplicates."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [hive-milvus.cursor :as cursor]
            [hive-milvus.store.entries :as entries]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]))

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
         :last-id       nil
         :last-batch-at nil
         :batch-size    100
         :cursor-path   nil
         :error-message nil}))

(def ^:private default-cursor-base
  (str (System/getProperty "user.home")
       "/.local/share/hive-mcp/relocate-cursor"))

(def ^:private default-batch-size 100)

(def ^:private failed-ids-cap 50)

(def ^:private default-source-collection "hive_mcp_memory")

(defn- list-ids-batch
  "Page of ids from `coll` strictly greater than `after-id` (lex ASC).
   Returns up to `limit` ids sorted ascending so resume is deterministic."
  [coll after-id limit]
  (let [filt (if (and after-id (not (str/blank? after-id)))
               (str "id > \"" after-id "\"")
               "id != \"\"")
        rows @(milvus/query-scalar coll
                {:filter            filt
                 :limit             limit
                 :output-fields     ["id"]
                 :consistency-level :bounded})]
    (->> rows (mapv :id) sort vec)))

(defn status
  "Return current relocation state snapshot + on-disk cursor."
  []
  (let [s @state
        c (when (:cursor-path s)
            (cursor/read-cursor (:cursor-path s)))]
    (-> s
        (dissoc :cursor-path)
        (assoc :cursor c))))

(defn stop!
  "Request the running relocation to stop after the current batch."
  []
  (if (= :running (:status @state))
    (do (swap! state assoc :stopping? true)
        {:requested-stop? true :was :running})
    {:requested-stop? false :was (:status @state)}))

(defn- record-result!
  [acc id result]
  (cond-> (-> acc (update :processed inc) (assoc :last-id id))
    (:moved? result)
    (update :moved inc)

    (and (false? (:moved? result)) (not (:error result)))
    (update :skipped inc)

    (:error result)
    (-> (update :failed inc)
        (update :failed-ids
                (fn [xs]
                  (cond-> xs
                    (< (count xs) failed-ids-cap) (conj id)))))))

(defn- run-batch!
  "Process `ids` concurrently with bounded parallelism (per-state
   :concurrency, default 1). Submits each id to a fixed thread pool,
   waits for all, then folds results in input order via `record-result!`
   so `:last-id` semantics are preserved (lex-ASC ids → final last-id
   is the max id confirmed processed).

   `:stopping?` is checked before each task starts; in-flight tasks
   complete (no mid-flight cancel of Milvus/Venice calls) so we may
   overshoot by up to (concurrency-1) entries before halting."
  [config-atom ids]
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
                                     [id (try (entries/relocate-entry! config-atom id)
                                              (catch Throwable e
                                                {:moved? false :error (.getMessage e) :id id}))]))))
                      ids)
            pairs   (mapv (fn [^java.util.concurrent.Future f] (.get f)) futures)]
        (reduce
          (fn [acc [id result]]
            (if (= ::stopping result)
              acc
              (record-result! acc id result)))
          {:processed 0 :moved 0 :skipped 0 :failed 0 :failed-ids []
           :last-id   nil}
          pairs))
      (finally
        (.shutdown ^java.util.concurrent.ExecutorService executor)))))

(defn- merge-batch-into-state!
  [batch-result]
  (swap! state
         (fn [s]
           (-> s
               (update :processed + (:processed batch-result))
               (update :moved     + (:moved     batch-result))
               (update :skipped   + (:skipped   batch-result))
               (update :failed    + (:failed    batch-result))
               (update :failed-ids
                       (fn [xs]
                         (->> (concat xs (:failed-ids batch-result))
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
  [config-atom coll cursor-path batch-size]
  (try
    (loop [last-id (or (:last-id @state) "")]
      (if (:stopping? @state)
        (finalize! cursor-path :stopped)
        (let [ids (list-ids-batch coll last-id batch-size)]
          (if (empty? ids)
            (finalize! cursor-path :completed)
            (let [batch-result (run-batch! config-atom ids)]
              (merge-batch-into-state! batch-result)
              (checkpoint-cursor! cursor-path)
              (log/info "relocate batch done"
                        {:coll coll :batch-size (count ids)
                         :moved (:moved batch-result)
                         :skipped (:skipped batch-result)
                         :failed (:failed batch-result)
                         :last-id (:last-id batch-result)})
              (recur (or (:last-id batch-result) last-id)))))))
    (catch Throwable e
      (log/error e "relocate runner failed for" coll)
      (swap! state assoc
             :status        :failed
             :error-message (.getMessage e)
             :last-batch-at (cursor/now-iso))
      (checkpoint-cursor! cursor-path))))

(defn start!
  "Spawn a background relocation pass. Returns immediately with the new
   job's metadata, or {:already-running? true} when a previous job is
   still :running. Resumes from the on-disk cursor when present."
  ([config-atom]
   (start! config-atom {}))
  ([config-atom {:keys [source-coll batch-size cursor-base concurrency]
                 :or   {source-coll  default-source-collection
                        batch-size   default-batch-size
                        cursor-base  default-cursor-base
                        concurrency  1}}]
   (if (= :running (:status @state))
     {:already-running? true :status (status)}
     (let [job-id      (str "reloc-" (System/currentTimeMillis))
           cursor-path (cursor/cursor-path-for cursor-base source-coll)
           _           (io/make-parents cursor-path)
           prior       (cursor/read-cursor cursor-path)
           resume-from (or (:last-id prior) "")]
       (reset! state {:job-id        job-id
                      :status        :running
                      :source-coll   source-coll
                      :started-at    (cursor/now-iso)
                      :stopping?     false
                      :processed     (or (:processed prior) 0)
                      :moved         (or (:moved     prior) 0)
                      :skipped       (or (:skipped   prior) 0)
                      :failed        0
                      :failed-ids    []
                      :last-id       resume-from
                      :last-batch-at nil
                      :batch-size    batch-size
                      :concurrency   concurrency
                      :cursor-path   cursor-path
                      :error-message nil})
       (future (runner-loop! config-atom source-coll cursor-path batch-size))
       {:job-id       job-id
        :source-coll  source-coll
        :cursor-path  cursor-path
        :resumed-from resume-from
        :batch-size   batch-size
        :concurrency  concurrency}))))

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
