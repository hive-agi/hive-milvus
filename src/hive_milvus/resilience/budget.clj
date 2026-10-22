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

(defn worst-case-write-ms
  "Longest a resilient write holds its caller: call + wait + retried call."
  []
  (+ write-call-timeout-ms reconnect-wait-ms write-call-timeout-ms))

(m/=> worst-case-write-ms [:=> [:cat] pos-int?])
