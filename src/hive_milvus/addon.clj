(ns hive-milvus.addon
  "IAddon implementation for Milvus vector database backend.

   Registers MilvusMemoryStore as the active IMemoryStore when initialized.
   Provides :vector-search capability to the addon registry.

   Config resolution is delegated to hive-milvus.config/resolve-MilvusConfig
   (a hive-di defconfig). The manifest-supplied config map is passed as
   overrides; missing keys fall back to MILVUS_* env vars then to typed
   defaults. All errors are collected (no short-circuit) and surfaced via
   the :errors vector in the IAddon initialize! result.

   Usage (from hive-mcp addon discovery or manual):
     (require '[hive-milvus.addon :as milvus-addon])
     (require '[hive-mcp.addons.core :as addons])

     (addons/register-addon! (milvus-addon/create-addon))
     (addons/init-addon! \"hive.milvus\"
       {:host \"milvus.milvus.svc\" :port 19530})"
  (:require [hive-mcp.addons.protocol :as addon-proto]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-milvus.config :as config]
            [hive-milvus.store :as store]
            [hive-dsl.result :as r]
            [taoensso.timbre :as log]
            [hive-milvus.relocate-addon :as reloc-addon]))

(defrecord MilvusAddon [store-atom]
  addon-proto/IAddon

  (addon-id [_this] "hive.milvus")

  (addon-type [_this] :native)

  (capabilities [_this]
    #{:vector-search :health-reporting})

  (initialize! [_this addon-config]
    (try
      (let [resolution (config/resolve-MilvusConfig (or addon-config {}))]
        (if (r/err? resolution)
          (let [errs (mapv (fn [{:keys [field message] :as e}]
                             (or message (pr-str (assoc e :field field))))
                           (:errors resolution))]
            (log/error "MilvusAddon config resolution failed" {:errors errs})
            {:success? false
             :errors (or (seq errs) [(pr-str resolution)])
             :metadata {:backend "milvus"}})
          (let [resolved      (:ok resolution)
                store-config  (select-keys resolved [:host :port :collection-name
                                                     :token :database :secure
                                                     :transport])
                store         (store/create-store store-config)
                connect-res   (mem-proto/connect! store resolved)]
            (if (:success? connect-res)
              (do
                (reset! store-atom store)
                (mem-proto/set-store! store)
                ;; Contribute relocate-* commands to the consolidated
                ;; `memory` MCP tool. Idempotent — safe across reloads.
                (try (reloc-addon/install!)
                     (catch Throwable e
                       (log/warn "Relocate addon install! failed (non-fatal):"
                                 (.getMessage e))))
                (log/info "MilvusAddon initialized — set as active memory store"
                          {:host      (:host resolved)
                           :port      (:port resolved)
                           :transport (:transport resolved)})
                {:success? true :errors [] :metadata {:backend "milvus"}})
              {:success? false
               :errors (:errors connect-res)
               :metadata {:backend "milvus"}}))))
      (catch Exception e
        {:success? false
         :errors [(.getMessage e)]
         :metadata {:backend "milvus"}})))

  (shutdown! [_this]
    (when-let [store @store-atom]
      (mem-proto/disconnect! store)
      (reset! store-atom nil)
      ;; Retract relocate-* commands so a fresh init re-contributes
      ;; cleanly.
      (try (reloc-addon/uninstall!)
           (catch Throwable e
             (log/debug "Relocate addon uninstall! failed (non-fatal):"
                        (.getMessage e))))
      (log/info "MilvusAddon shut down"))
    nil)

  (tools [_this] [])

  (schema-extensions [_this] {})

  (health [_this]
    (if-let [store @store-atom]
      (let [h (mem-proto/health-check store)]
        {:status  (if (:healthy? h) :ok :down)
         :details h})
      {:status :down
       :details {:reason "not initialized"}}))

  (excluded-tools [_this] #{}))

(defn create-addon
  "Create a MilvusAddon instance.

   Accepts an optional config map (passed by manifest init-from-manifest!).
   Actual configuration is applied during initialize!.

   Example:
     (require '[hive-mcp.addons.core :as addons])
     (addons/register-addon! (create-addon))
     (addons/init-addon! \"hive.milvus\"
       {:host \"milvus.milvus.svc\" :port 19530})"
  ([]
   (->MilvusAddon (atom nil)))
  ([_config]
   (->MilvusAddon (atom nil))))
