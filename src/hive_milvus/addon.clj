(ns hive-milvus.addon
  "IAddon implementation for Milvus vector database backend.

   Registers MilvusMemoryStore as the active IMemoryStore when initialized.
   Provides :vector-search capability to the addon registry.

   Usage (from hive-mcp addon discovery or manual):
     (require '[hive-milvus.addon :as milvus-addon])
     (require '[hive-mcp.addons.core :as addons])

     (addons/register-addon! (milvus-addon/create-addon))
     (addons/init-addon! \"hive.milvus\"
       {:host \"milvus.milvus.svc\" :port 19530})"
  (:require [hive-mcp.addons.protocol :as addon-proto]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-milvus.store :as store]
            [taoensso.timbre :as log]))

(defrecord MilvusAddon [store-atom]
  addon-proto/IAddon

  (addon-id [_this] "hive.milvus")

  (addon-type [_this] :native)

  (capabilities [_this]
    #{:vector-search :health-reporting})

  (initialize! [_this config]
    (try
      (let [;; Apply defaults for env vars that resolved to empty string
            resolved (-> config
                         (update :host #(if (seq %) % "localhost"))
                         (update :port #(if (and % (not= % ""))
                                          (if (string? %) (parse-long %) %)
                                          19530))
                         (update :collection-name #(if (seq %) % "hive_mcp_memory")))
            store (store/create-store
                    (select-keys resolved [:host :port :collection-name
                                           :token :database :secure]))
            result (mem-proto/connect! store resolved)]
        (if (:success? result)
          (do
            (reset! store-atom store)
            (mem-proto/set-store! store)
            (log/info "MilvusAddon initialized — set as active memory store"
                      {:host (:host config "localhost")
                       :port (:port config 19530)})
            {:success? true :errors [] :metadata {:backend "milvus"}})
          {:success? false
           :errors (:errors result)
           :metadata {:backend "milvus"}}))
      (catch Exception e
        {:success? false
         :errors [(.getMessage e)]
         :metadata {:backend "milvus"}})))

  (shutdown! [_this]
    (when-let [store @store-atom]
      (mem-proto/disconnect! store)
      (reset! store-atom nil)
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
