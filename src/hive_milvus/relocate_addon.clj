;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.relocate-addon
  "MCP-tool surface for hive-milvus.relocate.

   Contributes four subcommands to the consolidated `memory` MCP tool
   under addon-id `:hive.milvus.relocate`:

     - `relocate-start`        — kick off background pass
     - `relocate-status`       — read job + cursor state
     - `relocate-stop`         — graceful stop after current batch
     - `relocate-reset-cursor` — wipe on-disk cursor for a source coll

   Why scope this to a SEPARATE ns from hive-milvus.relocate:
     The runner (relocate.clj) speaks Clojure data; this addon shim
     speaks MCP (json text wrapper, error formatting, param coercion).
     Mixing them would couple the runner to MCP plumbing it doesn't
     need."
  (:require [hive-spi.memory.registry :as mem-reg]
            [hive-milvus.relocate :as reloc]
            [taoensso.timbre :as log]
            [hive-addon.host :as host]
            [hive-dsl.result :as r]))

;; =============================================================================
;; Host services
;;
;; The extension registry and the MCP response formatters belong to the host.
;; They are resolved through the var at call time so this namespace loads with
;; no host present; every function below is only ever REACHED through the host's
;; own tool dispatch, so the degraded (r/err :host/absent ..) return is a
;; diagnostic, not a path the addon takes in normal operation.
;; =============================================================================

(host/defsoft contribute-commands! 'hive-mcp.extensions.registry/contribute-commands!)
(host/defsoft retract-commands! 'hive-mcp.extensions.registry/retract-commands!)
(host/defsoft mcp-json 'hive-mcp.tools.core/mcp-json)
(host/defsoft mcp-error 'hive-mcp.tools.core/mcp-error)

(defn- store-config-atom
  "Extract the `:config-atom` field from the active MilvusMemoryStore.
   Returns nil if no store is registered or if the registered store
   isn't a Milvus instance."
  []
  (when-let [store (mem-reg/get-store)]
    (:config-atom store)))

(defn handle-relocate-start
  "Spawn a background relocation pass. Returns immediately.

   Params (all optional):
     source-coll  — Milvus source collection (default: hive_mcp_memory)
     batch-size   — ids per Milvus query page (default: 100)
     cursor-base  — cursor file prefix"
  [{:keys [source-coll batch-size cursor-base]}]
  (if-let [config-atom (store-config-atom)]
    (let [opts (cond-> {}
                 source-coll (assoc :source-coll source-coll)
                 batch-size  (assoc :batch-size  (long batch-size))
                 cursor-base (assoc :cursor-base cursor-base))
          result (reloc/start! config-atom opts)]
      (mcp-json result))
    (mcp-error "Relocate requires the Milvus store to be active. No active store found via mem-reg/get-store.")))

(defn handle-relocate-status
  "Return current relocation state snapshot + on-disk cursor."
  [_params]
  (mcp-json (reloc/status)))

(defn handle-relocate-stop
  "Request the running relocation to stop after the current batch."
  [_params]
  (mcp-json (reloc/stop!)))

(defn handle-relocate-reset-cursor
  "Delete on-disk cursor for a source collection. Use after `stop!`
   when re-running from scratch."
  [{:keys [source-coll cursor-base]}]
  (let [args (cond-> []
               source-coll (conj source-coll)
               cursor-base (conj cursor-base))
        result (apply reloc/reset-cursor! args)]
    (mcp-json result)))

(def ^:private relocate-commands
  "The four subcommands this addon contributes to the `memory` tool."
  {"relocate-start"
    {:handler     handle-relocate-start
     :params      {"source-coll" {:type "string"
                                  :description "Milvus source collection (default: hive_mcp_memory)"}
                   "batch-size"  {:type "integer"
                                  :description "Ids per Milvus query page (default: 100)"}
                   "cursor-base" {:type "string"
                                  :description "Cursor file prefix (default: ~/.local/share/hive-mcp/relocate-cursor)"}}
     :description "Spawn a background relocation pass that drains entries from a non-canonical Milvus collection (e.g. legacy 768-d hive_mcp_memory) into per-dim collections via routing. Resumable — picks up from last checkpoint."}

    "relocate-status"
    {:handler     handle-relocate-status
     :params      {}
     :description "Return the current relocation job state + on-disk cursor (processed, moved, skipped, failed counts; last-id; status :idle|:running|:completed|:stopped|:failed)."}

    "relocate-stop"
    {:handler     handle-relocate-stop
     :params      {}
     :description "Request the running relocation pass to stop after the current batch. State persists; rerun via relocate-start to resume from cursor."}

    "relocate-reset-cursor"
    {:handler     handle-relocate-reset-cursor
     :params      {"source-coll" {:type "string"
                                  :description "Source collection whose cursor to delete"}
                   "cursor-base" {:type "string"
                                  :description "Override cursor file prefix"}}
     :description "Delete the on-disk cursor for a source collection. Call after relocate-stop when re-running from scratch (e.g. after wiping target collections in dev)."}})

(defn install!
  "Contribute relocate-* commands to the consolidated `memory` tool.
   Idempotent — safe to call repeatedly. Should run AFTER MilvusAddon
   initializes so the active store is reachable when handlers fire.

   Returns the registry's result. With no host on the classpath there is no
   registry to contribute to, and that is reported as an :host/absent err
   rather than logged as a successful install."
  []
  (let [result (contribute-commands! "memory" :hive.milvus.relocate relocate-commands)]
    (if (r/err? result)
      (log/warn "hive-milvus.relocate-addon: memory/relocate-* NOT installed:" result)
      (log/info "hive-milvus.relocate-addon installed memory/relocate-* commands"))
    result))

(defn uninstall!
  "Remove all relocate-* contributions from the memory tool.
   Returns the registry's result, which is an :host/absent err when no host is
   on the classpath to retract from."
  []
  (let [result (retract-commands! "memory" :hive.milvus.relocate)]
    (if (r/err? result)
      (log/warn "hive-milvus.relocate-addon: nothing retracted:" result)
      (log/info "hive-milvus.relocate-addon uninstalled"))
    result))
