;; PROPRIETARY - Copyright 2026 BuddhiLW. All Rights Reserved.
;; This file is part of hive-milvus and may not be distributed
;; without explicit written permission.

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
  (:require [hive-mcp.extensions.registry :as ext]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-mcp.tools.core :refer [mcp-json mcp-error]]
            [hive-milvus.relocate :as reloc]
            [taoensso.timbre :as log]))

(defn- store-config-atom
  "Extract the `:config-atom` field from the active MilvusMemoryStore.
   Returns nil if no store is registered or if the registered store
   isn't a Milvus instance."
  []
  (when-let [store (mem-proto/get-store)]
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
    (mcp-error "Relocate requires the Milvus store to be active. No active store found via mem-proto/get-store.")))

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

(defn install!
  "Contribute relocate-* commands to the consolidated `memory` tool.
   Idempotent — safe to call repeatedly. Should run AFTER MilvusAddon
   initializes so the active store is reachable when handlers fire."
  []
  (ext/contribute-commands!
   "memory" :hive.milvus.relocate
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

  (log/info "hive-milvus.relocate-addon installed memory/relocate-* commands"))

(defn uninstall!
  "Remove all relocate-* contributions from the memory tool."
  []
  (ext/retract-commands! "memory" :hive.milvus.relocate)
  (log/info "hive-milvus.relocate-addon uninstalled"))
