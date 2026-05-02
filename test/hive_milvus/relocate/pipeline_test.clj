(ns hive-milvus.relocate.pipeline-test
  "Trifecta + focused regression coverage for `relocate-update`.

   Pins the let-ok short-circuit bug caught 2026-05-02: when an
   `r/let-ok` binding evaluated to a plain map (not a Result), the
   macro silently terminated the chain — `milvus-write!` never fired
   and kanban moves looked successful at the handler boundary while
   the underlying store kept the old tags.

   The trifecta target is the test-only `run-update` harness, which
   stubs the COLLECT/PROMOTE/BOUNDARY layers and reports what the
   write boundary received. A successful update must produce a
   captured record whose keys reflect the requested updates."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-dsl.result :as r]
            [hive-milvus.relocate.boundary :as boundary]
            [hive-milvus.relocate.collectors.entry :as col-entry]
            [hive-milvus.relocate.pipeline :as p]
            [hive-milvus.relocate.promoters.record :as p-record]
            [hive-milvus.relocate.promoters.routing :as p-routing]
            [hive-test.trifecta :refer [deftrifecta]]))

;; ============================================================
;; Harness — single-arg fn so `deftrifecta` can target it directly
;; ============================================================

(defn run-update
  "Execute `relocate-update` against stubbed COLLECT/PROMOTE/BOUNDARY
   layers and return the record passed to `milvus-write!`, or nil if
   the boundary never fired.

   Stubs are deterministic (no embedder, no Milvus) so the captured
   record is a function of `(entry, updates)` alone — golden-stable."
  [{:keys [entry updates]}]
  (let [captured (atom nil)]
    (with-redefs
      [col-entry/collect-existing-entry
       (fn [_] (r/ok {:id          (:id entry)
                      :src-coll    "stub-coll"
                      :entry       entry
                      :config-atom :stub}))

       p-routing/compute-target
       (fn [bundle] (r/ok (assoc bundle :target-coll "stub-coll")))

       p-routing/classify-relocation-need
       (fn [bundle] (r/ok (assoc bundle :relocation :no-op)))

       boundary/ensure-target-collection
       (fn [bundle] (r/ok bundle))

       p-record/build-target-record
       (fn [bundle]
         (r/ok (assoc bundle
                      :embedding [0.0]
                      :record    (:entry bundle))))

       boundary/milvus-write!
       (fn [bundle-result]
         (reset! captured (:record (:ok bundle-result)))
         bundle-result)]
      (p/relocate-update :stub (:id entry) updates)
      @captured)))

;; ============================================================
;; Invariant for property facet
;; ============================================================
;;
;; The property body sees `(run-update input)` — the captured record.
;; Non-nil = boundary fired = bug not triggered. The mutation facet
;; catches *shape* divergence (keep-old, empty-out) by comparing the
;; captured record against the golden snapshot.

;; ============================================================
;; Generators
;; ============================================================

(def ^:private gen-id     (gen/elements ["k1" "k2" "k3"]))
(def ^:private gen-status (gen/elements ["todo" "doing" "review" "done"]))

(def ^:private gen-entry+updates
  (gen/let [id     gen-id
            old-st gen-status
            new-st gen-status]
    {:entry   {:id      id
               :type    "note"
               :content {:status old-st :title "t"}
               :tags    ["kanban" old-st "scope:project:hive"]}
     :updates {:content {:status new-st :title "t"}
               :tags    ["kanban" new-st "scope:project:hive"]
               :updated "2026-05-02T12:00:00-0300"}}))

;; ============================================================
;; Mutation oracles — broken impls of `run-update`
;; ============================================================

(defn- mut-no-write
  "The exact regression we're pinning: pipeline returns without
   firing the write boundary. Captured stays nil."
  [_] nil)

(defn- mut-keep-old
  "Merge step ignored — write fires but with the pre-update entry."
  [{:keys [entry]}] entry)

(defn- mut-empty
  "Write fires with garbage."
  [_] {})

;; ============================================================
;; Trifecta — golden + property + mutation in one declaration
;; ============================================================

(deftrifecta relocate-update-write-trifecta
  hive-milvus.relocate.pipeline-test/run-update
  {:golden-path "test/golden/milvus/trifecta-relocate-update-write.edn"
   :cases       {:todo->doing
                 {:entry   {:id      "k1"
                            :type    "note"
                            :content {:status "todo" :title "T"}
                            :tags    ["kanban" "todo" "scope:project:hive"]}
                  :updates {:content {:status "doing" :title "T"
                                      :started "2026-05-02T12:00:00-0300"}
                            :tags    ["kanban" "doing" "scope:project:hive"]
                            :updated "2026-05-02T12:00:00-0300"}}

                 :doing->done
                 {:entry   {:id      "k2"
                            :type    "note"
                            :content {:status "doing"}
                            :tags    ["kanban" "doing"]}
                  :updates {:content {:status    "done"
                                      :completed "2026-05-02T12:00:00-0300"}
                            :tags    ["kanban" "done"]
                            :updated "2026-05-02T12:00:00-0300"}}

                 :metadata-only
                 {:entry   {:id      "k3"
                            :type    "note"
                            :content {:status "doing"}
                            :tags    ["kanban" "doing"]}
                  :updates {:kg-incoming ["edge1" "edge2"]
                            :updated     "2026-05-02T12:00:00-0300"}}}
   :gen         gen-entry+updates
   :pred        some?
   :num-tests   100
   :mutations   [["no-write"  mut-no-write]
                 ["keep-old"  mut-keep-old]
                 ["empty-out" mut-empty]]})

;; ============================================================
;; Focused regression — explicit, readable failure on the original bug
;; ============================================================

(deftest let-ok-short-circuit-regression
  (testing "relocate-update fires milvus-write! even when r/let-ok bindings
            include non-Result pure expressions"
    (let [entry    {:id      "k-reg"
                    :type    "note"
                    :content {:status "todo"}
                    :tags    ["kanban" "todo" "scope:project:hive"]}
          updates  {:content {:status "doing"}
                    :tags    ["kanban" "doing" "scope:project:hive"]}
          captured (run-update {:entry entry :updates updates})]
      (is (some? captured)
          "milvus-write! never fired — pipeline aborted on a non-Result binding")
      (is (= (:tags updates) (:tags captured))
          "captured record must carry the updated tags")
      (is (= (:content updates) (:content captured))
          "captured record must carry the updated content"))))
