(ns latency-bench
  "Scratch-only interleaved update benchmark. Run in an isolated JVM.
   Credentials are read from a private EDN file, never printed."
  (:require [clojure.edn :as edn]
            [milvus-clj.api :as milvus]
            [milvus-clj.schema :as ms]
            [milvus-clj.index :as mi]
            [hive-dsl.result :as r]
            [hive-milvus.store.query :as query]
            [hive-milvus.store.schema :as schema]
            [hive-milvus.relocate.collectors.entry :as collector]
            [hive-milvus.relocate.boundary :as boundary]
            [hive-milvus.relocate.promoters.record :as record]
            [hive-milvus.relocate.pipeline :as pipeline]
            [hive-milvus.embed.port :as port]
            [hive-milvus.store.index :as index]
            [hive-milvus.store.entries :as entries]))

(defn timed [f]
  (let [t (System/nanoTime) value (f)]
    {:ms (/ (- (System/nanoTime) t) 1e6) :value value}))

(defn checked [res]
  (if (r/ok? res) (:ok res) (throw (ex-info "benchmark operation failed" res))))

(defn update-once [coll id arm i]
  (let [read (timed #(if (= arm :before)
                       (when-let [e (query/get-entry-by-id coll id)] {:entry e})
                       (query/get-entry-with-vector coll id)))
        snapshot (:value read)
        existing (:entry snapshot)
        merged (assoc existing :tags [(str "round-" i)])
        bundle (cond-> {:id id :src-coll coll :target-coll coll
                        :entry merged :existing existing}
                 (= arm :after) (assoc :stored-embedding (:stored-embedding snapshot)))
        keep (timed #(checked (boundary/keep-stored-vector bundle)))
        built (checked (record/build-target-record (:value keep)))
        write (timed #(checked (boundary/milvus-write! (r/ok built))))]
    (assert (= (vec (repeat 2560 0.125)) (:embedding built)))
    (assert (not (contains? merged :embedding)))
    {:arm arm :collect-ms (:ms read) :keep-ms (:ms keep)
     :upsert-ms (:ms write) :total-ms (+ (:ms read) (:ms keep) (:ms write))}))

(defrecord ScratchEmbedder [coll calls]
  port/IEmbedder
  (-embed-entry [_ _ _ _] (swap! calls inc) (r/ok (vec (repeat 2560 0.25))))
  (-embed-text [_ _ _] (vec (repeat 2560 0.25)))
  (-routing-for-type [_ _] {:collection-name coll :dimension 2560})
  (-routing-for-type+size [_ _ _] {:collection-name coll :dimension 2560})
  (-no-embed-type? [_ _] false)
  (-collection-names [_] [coll])
  (-dimension-for-collection [_ _] 2560)
  (-configured-collection-names [_] [coll])
  (-provider-available-for? [_ _] true)
  (-collection-backed? [_ c] (= coll c)))

(defn pipeline-smoke! [coll id]
  (let [calls (atom 0) config (atom {:collection-name coll})]
    (port/set-embedder! (->ScratchEmbedder coll calls))
    ;; Scratch is already created and loaded. HTTP cannot create scalar
    ;; indexes; pre-seed only this isolated process's setup memo.
    (swap! index/loaded-collections conj coll)
    (swap! index/indexed-collections conj coll)
    (try
      (let [res (pipeline/relocate-update config id {:tags ["pipeline"] :id "cannot-rename"})]
        (assert (r/ok? res))
        (assert (= id (:id (:ok res))))
        (assert (= ["pipeline"] (:tags (:ok res))))
        (assert (not (contains? (:ok res) :stored-embedding)))
        (assert (zero? @calls)))
      (Thread/sleep 6000)
      (let [res (pipeline/relocate-update config id {:content "changed"})]
        (assert (r/ok? res))
        (assert (= 1 @calls)))
      (Thread/sleep 6000)
      (let [snapshot (query/get-entry-with-vector coll id)]
        (assert (= "changed" (get-in snapshot [:entry :content])))
        (assert (= ["pipeline"] (get-in snapshot [:entry :tags])))
        (assert (= (vec (repeat 2560 0.25)) (:stored-embedding snapshot))))
      (println :pipeline-smoke-ok true)
      (let [rows (mapv (fn [i]
                         (let [t (timed #(entries/update-fields-keep-embedding!
                                          config id {:tags [(str "metadata-" i)]}))]
                           (assert (= id (:id (:value t))))
                           (:ms t)))
                       (range 5))]
        (println :metadata-update-ms rows))
      (finally
        (port/reset-embedder!)
        (swap! index/loaded-collections disj coll)
        (swap! index/indexed-collections disj coll)))))

(defn run! [connection-file]
  (let [coll (str "cx_latency_" (System/currentTimeMillis))
        id "scratch-entry"
        entry {:id id :type :note :content (apply str (repeat 1024 "x"))
               :tags ["scratch"] :project-id "scratch"}
        vector (vec (repeat 2560 0.125))]
    (milvus/connect! (edn/read-string (slurp connection-file)))
    (try
      @(milvus/create-collection coll {:schema (ms/with-dimension 2560)
                                       :index mi/default-memory-index})
      (println :scratch-created coll)
      (println :initial-upsert-ms (:ms (timed #(deref (milvus/add coll [(schema/entry->record-pure entry coll vector)] :upsert? true)))))
      (Thread/sleep 6000)
      (update-once coll id :before -1)
      (update-once coll id :after -1)
      (let [rows (vec (for [i (range 8)
                            arm (if (even? i) [:before :after] [:after :before])]
                        (let [row (update-once coll id arm i)]
                          (println (pr-str row)) row)))
            summarize (fn [arm]
                        (let [rs (filter #(= arm (:arm %)) rows)]
                          (into {:n (count rs)}
                                (for [k [:collect-ms :keep-ms :upsert-ms :total-ms]]
                                  [k (/ (reduce + (map k rs)) (count rs))]))))]
        (println :summary (pr-str {:before (summarize :before) :after (summarize :after)}))
        ;; Verify final write by a strong query directly (HTTP adapter currently
        ;; omits consistencyLevel, so give the default bounded view time).
        (Thread/sleep 6000)
        (let [snapshot (query/get-entry-with-vector coll id)]
          (assert (= ["round-7"] (get-in snapshot [:entry :tags])))
          (assert (= vector (:stored-embedding snapshot))))
        (do (println :roundtrip-ok true) (pipeline-smoke! coll id)))
      (finally
        @(milvus/drop-collection coll)
        (println :scratch-dropped coll)
        (milvus/disconnect!)))))
