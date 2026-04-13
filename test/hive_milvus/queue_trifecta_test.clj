(ns hive-milvus.queue-trifecta-test
  "Trifecta tests (golden + property + mutation) for hive-milvus.queue
   pure functions: `coalesce` and `degraded-response`.

   These cover the dedupe-key invariants and the read-fail-soft response
   shape that the existing queue-test.clj only example-tests. Trifecta
   collapses three facets into one declaration per function."
  (:require [clojure.test :refer [use-fixtures]]
            [clojure.test.check.generators :as gen]
            [hive-test.trifecta :refer [deftrifecta]]
            [hive-milvus.queue :as q]))

(use-fixtures :each
  (fn [t]
    (q/clear!)
    (t)
    (q/clear!)))

;; =============================================================================
;; Generators
;; =============================================================================

(def ^:private gen-op-kw
  (gen/elements [:add-entry! :update-entry! :delete-entry!
                 :log-access! :record-feedback! :update-staleness!
                 :cleanup-expired!]))

(def ^:private gen-id
  ;; Small, repeat-prone id space so coalescing actually happens under
  ;; the property gen (large random ids would never collide).
  (gen/elements ["a" "b" "c" "d" "e"]))

(def ^:private gen-op
  (gen/let [op    gen-op-kw
            id?   gen/boolean
            id    gen-id]
    (cond-> {:op op :args []}
      id? (assoc :id id))))

(def ^:private gen-op-batch
  (gen/vector gen-op 0 25))

;; =============================================================================
;; Invariants
;; =============================================================================

(defn- coalesce-key-of
  "Mirror of private q/coalesce-key — kept in the test so we assert
   against the invariant directly, not via the function under test."
  [op]
  (if-let [id (:id op)]
    [(:op op) id]
    [:singleton (:op op)]))

(defn- coalesce-invariants
  "The three invariants `coalesce` must satisfy for any input batch:
   (1) output is a vector, (2) distinct by coalesce-key, (3) every
   output element was present in the input (no fabricated ops)."
  [input]
  (let [out    (q/coalesce input)
        keys'  (map coalesce-key-of out)
        in-set (set input)]
    (and (vector? out)
         (<= (count out) (count input))
         (= (count out) (count (set keys')))
         (every? in-set out))))

(defn- degraded-response-valid?
  [resp]
  (and (map? resp)
       (false? (:success? resp))
       (true?  (:degraded? resp))
       (= "milvus" (:backend resp))
       (true?  (:reconnecting? resp))
       (vector? (:tips resp))
       (seq (:tips resp))
       (number? (:retry-after-ms resp))
       (integer? (:queue-depth resp))))

;; =============================================================================
;; 1. coalesce — golden + property + mutation
;; =============================================================================

(deftrifecta coalesce-trifecta
  hive-milvus.queue/coalesce
  {:golden-path "test/golden/milvus/trifecta-queue-coalesce.edn"
   :cases       {:empty        []
                 :singleton    [{:op :cleanup-expired! :args []}]
                 :three-ids    [{:op :add-entry!    :id "a" :args [{:v 1}]}
                                {:op :update-entry! :id "b" :args ["b" {:v 2}]}
                                {:op :delete-entry! :id "c" :args ["c"]}]
                 :repeat-add   [{:op :add-entry! :id "a" :args [{:v 1}]}
                                {:op :add-entry! :id "a" :args [{:v 2}]}
                                {:op :add-entry! :id "a" :args [{:v 3}]}]
                 :interleaved  [{:op :add-entry!    :id "a" :args [{:v 1}]}
                                {:op :add-entry!    :id "b" :args [{:v 2}]}
                                {:op :add-entry!    :id "a" :args [{:v 10}]}
                                {:op :update-entry! :id "b" :args ["b" {:v 20}]}]
                 :singletons-collapse
                 [{:op :cleanup-expired! :args []}
                  {:op :cleanup-expired! :args []}
                  {:op :cleanup-expired! :args []}]
                 :mixed-with-singleton
                 [{:op :add-entry!       :id "a" :args [{:v 1}]}
                  {:op :cleanup-expired! :args []}
                  {:op :add-entry!       :id "a" :args [{:v 2}]}
                  {:op :cleanup-expired! :args []}]}
   :gen         gen-op-batch
   :pred        coalesce-invariants
   :num-tests   200
   :mutations   [["always-empty"  (fn [_] [])]
                 ["passthrough"   (fn [ops] (vec ops))]
                 ["reverse-order" (fn [ops] (vec (reverse (q/coalesce ops))))]
                 ["first-wins"
                  ;; Broken: keep first occurrence instead of latest. For
                  ;; cases with repeats this diverges from the golden.
                  (fn [ops]
                    (->> ops
                         (reduce (fn [acc op]
                                   (let [k (if-let [id (:id op)]
                                             [(:op op) id]
                                             [:singleton (:op op)])]
                                     (if (contains? (:seen acc) k)
                                       acc
                                       (-> acc
                                           (update :seen conj k)
                                           (update :out conj op)))))
                                 {:seen #{} :out []})
                         :out))]]})

;; =============================================================================
;; 2. degraded-response — golden + property + mutation
;;    Note: :queue-depth reflects (size) at call time. The :each fixture
;;    clears the queue so depth is 0 across cases and property runs.
;; =============================================================================

(deftrifecta degraded-response-trifecta
  hive-milvus.queue/degraded-response
  {:golden-path  "test/golden/milvus/trifecta-queue-degraded-response.edn"
   :cases        {:defaults         {}
                  :with-operation   {:operation :search-similar}
                  :with-retry-after {:retry-after-ms 30000}
                  :with-eta         {:retry-after-ms 1000
                                     :reconnect-eta-ms 8000
                                     :operation :get-entry}
                  :with-extra-tips  {:extra-tips ["Check tailscale status"
                                                   "kubectl get pods -n milvus"]}}
   ;; Tips count varies with :reconnect-eta-ms presence, so snapshot
   ;; :tips as a tip-count summary rather than the full strings. Keeps
   ;; goldens stable across wording tweaks.
   :xf           (fn [resp]
                   (-> resp
                       (dissoc :tips)
                       (assoc :tip-count (count (:tips resp)))))
   :gen          (gen/let [retry (gen/choose 100 60000)
                           eta?  gen/boolean
                           eta   (gen/choose 100 120000)
                           op?   gen/boolean
                           op    gen/keyword]
                   (cond-> {:retry-after-ms retry}
                     eta? (assoc :reconnect-eta-ms eta)
                     op?  (assoc :operation op)))
   :pred         degraded-response-valid?
   :num-tests    150
   :mutations    [["drop-tips"
                   (fn [opts]
                     (dissoc (q/degraded-response opts) :tips))]
                  ["wrong-backend"
                   (fn [opts]
                     (assoc (q/degraded-response opts) :backend "chroma"))]
                  ["success-true"
                   (fn [opts]
                     (assoc (q/degraded-response opts) :success? true))]
                  ["empty-map" (fn [_] {})]]})
