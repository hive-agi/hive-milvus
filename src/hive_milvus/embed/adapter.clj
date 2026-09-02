;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.embed.adapter
  "Binds `hive-milvus.embed.port/IEmbedder` to the host's embedding service,
   resilient failover chain and Chroma embed call. The host installs it at
   addon-init via `install!`; until then the core runs on the port's
   NoopEmbedder.

   Every host call is soft-resolved through the var at call time
   (hive-addon.host), so this namespace loads with no host on the classpath.
   Each one declares its own degraded value, matching the type its call site
   expects: a provider lookup gives nil, a predicate gives false, a collection
   listing gives nil, and an embed call gives an :embedder/host-absent Result.
   A blanket (r/err ..) would be WRONG here, because a map is truthy and would
   be threaded on as if it were a provider or a chain."
  (:require [hive-addon.host :as host]
            [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]))

;; =============================================================================
;; Host services
;; =============================================================================

(host/defsoft resolve-provider-for-type
  'hive-mcp.embeddings.service/resolve-provider-for-type :absent (constantly nil))

(host/defsoft resolve-provider-for-type+size
  'hive-mcp.embeddings.service/resolve-provider-for-type+size :absent (constantly nil))

(host/defsoft resolve-provider-chain-for-type+size
  'hive-mcp.embeddings.service/resolve-provider-chain-for-type+size :absent (constantly nil))

(host/defsoft get-provider-for
  'hive-mcp.embeddings.service/get-provider-for :absent (constantly nil))

(host/defsoft embed-for-collection
  'hive-mcp.embeddings.service/embed-for-collection
  :absent (fn [collection-name _text]
            (r/err :embedder/host-absent {:collection-name collection-name})))

(host/defsoft no-embed-type?
  'hive-mcp.embeddings.service/no-embed-type? :absent (constantly false))

(host/defsoft type->collection-names
  'hive-mcp.embeddings.service/type->collection-names :absent (constantly nil))

(host/defsoft get-dimension-for
  'hive-mcp.embeddings.service/get-dimension-for :absent (constantly nil))

(host/defsoft list-configured-collections
  'hive-mcp.embeddings.service/list-configured-collections :absent (constantly nil))

(host/defsoft provider-available-for?
  'hive-mcp.embeddings.service/provider-available-for? :absent (constantly false))

(host/defsoft collection-backed?
  'hive-mcp.embeddings.service/collection-backed? :absent (constantly false))

(host/defsoft resilient-embedder
  'hive-mcp.embeddings.resilient/resilient-embedder :absent (constantly nil))

(host/defsoft embed-text
  'hive-mcp.chroma.embeddings/embed-text
  :absent (fn [_embedder _content]
            (throw (ex-info "embed-text unavailable: host absent"
                            {:error :embedder/host-absent}))))

;; =============================================================================
;; Adapter
;; =============================================================================

(defn- routing-map [provider]
  (when provider
    (select-keys provider [:collection-name :dimension :max-tokens :provider-key])))

(defn- resolve-provider [entry collection-name]
  (or (some-> (:type entry)
              resolve-provider-for-type
              :provider)
      (when collection-name
        (try (get-provider-for collection-name)
             (catch Exception _ nil)))))

(defn- resilient-embedder-for [entry collection-name content]
  (let [chain (some-> (:type entry)
                      (resolve-provider-chain-for-type+size content))]
    (if (seq chain)
      (resilient-embedder chain)
      (when-let [p (resolve-provider entry collection-name)]
        (resilient-embedder [{:provider p :provider-key :collection-fallback}])))))

(defrecord HiveMcpEmbedder []
  port/IEmbedder
  (-embed-entry [_ entry collection-name content]
    (if-let [embedder (resilient-embedder-for entry collection-name content)]
      (r/try-effect* :embedder/embed-failed
        (embed-text embedder content))
      (r/err :embedder/no-provider
             {:type            (:type entry)
              :collection-name collection-name})))
  (-embed-text [_ collection-name text]
    (embed-for-collection collection-name text))
  (-routing-for-type [_ memory-type]
    (routing-map (resolve-provider-for-type memory-type)))
  (-routing-for-type+size [_ memory-type content]
    (routing-map (resolve-provider-for-type+size memory-type content)))
  (-no-embed-type? [_ memory-type]
    (no-embed-type? memory-type))
  (-collection-names [_]
    (type->collection-names nil))
  (-dimension-for-collection [_ collection-name]
    (get-dimension-for collection-name))
  (-configured-collection-names [_]
    (keys (list-configured-collections)))
  (-provider-available-for? [_ collection-name]
    (provider-available-for? collection-name))
  (-collection-backed? [_ collection-name]
    (collection-backed? collection-name)))

(defn available?
  "True when the host's embedding service is on the classpath right now."
  []
  (host/available? 'hive-mcp.embeddings.service/resolve-provider-for-type))

(defn install!
  "Install the host-backed embedder into the port slot."
  []
  (port/set-embedder! (->HiveMcpEmbedder)))

(defn uninstall!
  []
  (port/reset-embedder!))
