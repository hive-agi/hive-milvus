(ns hive-milvus.resilience.budget
  "Time budgets for Milvus calls, sized to fit inside hive-mcp's
   memory-add stage budgets (the :index stage is 12 s).

   A resilient write holds its caller for at most one call, one bounded
   reconnect wait and one retried call; `worst-case-write-ms` sums them and
   must stay under `stage-budget-ms`, so hive-mcp's stage error never fires
   while this layer is still waiting."
  (:require [malli.core :as m]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(def stage-budget-ms
  "hive-mcp's memory-add :index stage budget."
  12000)

(def write-call-timeout-ms
  "Deref bound for one write RPC (upsert)."
  4000)

(def reconnect-wait-ms
  "How long a failed call waits for verified recovery before its one retry."
  3000)

(def reconnect-connect-timeout-ms
  "Transport connect timeout the heal loop dials with: one attempt fits
   inside `reconnect-wait-ms`."
  3000)

(def warmup-timeout-ms
  "Bound on the boot warm-up probe."
  3000)

(def read-deadline-cap-ms
  "No read's deadline scales past this: a bigger ask is paged, not waited for."
  30000)

(def search-row-cost-ms
  "Measured 2026-10-24: a vector search costs about 16 ms per returned row."
  20)

(def scalar-row-cost-ms
  "Allowance per row of a scalar scan that returns content."
  1)

(def projected-row-cost-ms
  "Allowance per row of a scalar scan that returns scalar fields only."
  0.25)

(defn read-deadline-ms
  "Deref bound for a read that asks for `rows` rows at `row-cost-ms` each: the
   floor `base-ms` plus the cost of the ask, never above `read-deadline-cap-ms`
   (nor below the floor). A small read keeps the short default; a large one gets
   the time its size needs instead of an empty answer at the floor."
  [base-ms rows row-cost-ms]
  (let [scaled (long (Math/ceil (* (double (max 0 (or rows 0))) (double row-cost-ms))))]
    (long (max base-ms (min read-deadline-cap-ms (+ base-ms scaled))))))

(defn worst-case-write-ms
  "Longest a resilient write holds its caller: call + wait + retried call."
  []
  (+ write-call-timeout-ms reconnect-wait-ms write-call-timeout-ms))

(m/=> worst-case-write-ms [:=> [:cat] pos-int?])
