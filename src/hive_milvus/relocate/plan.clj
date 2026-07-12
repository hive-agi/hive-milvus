;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

(ns hive-milvus.relocate.plan
  "Pure decisions for a drain pass: which rows to ask for next, and what a
   finished round means for the loop. Plain data in, plain data out — no
   client, no clock, no state."
  (:require [clojure.string :as str]
            [malli.core :as m]))

;; =============================================================================
;; Value Objects
;; =============================================================================

(def Id
  "An entry id as it may appear inside a Milvus filter expression."
  [:re #"^[A-Za-z0-9_.:@+-]+$"])

(def Ids [:sequential Id])

(def Count [:int {:min 0 :max 100000}])

(def default-max-excluded
  "Rows the drain may carry as unmovable before it stops and asks for a human."
  500)

(def Verdict
  [:enum :continue :completed :stopped :stalled])

(def Round
  "Everything the loop knows once a round is over."
  [:map
   [:page-count     Count]
   [:classified     Count]
   [:excluded-count Count]
   [:max-excluded   Count]
   [:stopping?      :boolean]])

;; =============================================================================
;; Decisions
;; =============================================================================

(def ^:private id-validator (m/validator Id))

(defn exclusion-filter
  "Milvus scalar filter selecting every row whose id is not in `excluded`.

   Throws when an id cannot be safely embedded in a filter expression: a
   filter that silently drops an id would hand the same row back forever."
  [excluded]
  (if-let [bad (seq (remove id-validator excluded))]
    (throw (ex-info "Entry id is not safe to embed in a Milvus filter"
                    {:error :relocate/unsafe-id :ids (vec bad)}))
    (if (seq excluded)
      (str "id not in ["
           (str/join ", " (map #(str "\"" % "\"") (sort excluded)))
           "]")
      "id != \"\"")))

(defn verdict
  "What the loop must do after a round.

   `:completed` means the source handed back nothing that is not already
   excluded — never that every row moved. The excluded ids are the report."
  [{:keys [page-count classified excluded-count max-excluded stopping?]}]
  (cond
    stopping?                       :stopped
    (> excluded-count max-excluded) :stalled
    (zero? page-count)              :completed
    (zero? classified)              :stalled
    :else                           :continue))

;; =============================================================================
;; Contracts
;; =============================================================================

(m/=> exclusion-filter [:=> [:cat Ids] :string])

(m/=> verdict [:=> [:cat Round] Verdict])
