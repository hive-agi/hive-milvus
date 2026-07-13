;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.embed.adapter
  "HOST-INTEGRATION (excluded from the published Maven-clean core).

   Binds `hive-milvus.embed.port/IEmbedder` to hive-mcp's embedding
   service, resilient failover chain and Chroma embed call. The host
   installs it at addon-init via `install!`; until then the core runs
   on the port's NoopEmbedder."
  (:require [hive-dsl.result :as r]
            [hive-mcp.embeddings.service :as embed-svc]
            [hive-mcp.embeddings.resilient :as resilient]
            [hive-milvus.embed.port :as port]))

(defn- routing-map [provider]
  (when provider
    (select-keys provider [:collection-name :dimension :max-tokens :provider-key])))

(defn- resolve-provider [entry collection-name]
  (or (some-> (:type entry)
              embed-svc/resolve-provider-for-type
              :provider)
      (when collection-name
        (try (embed-svc/get-provider-for collection-name)
             (catch Exception _ nil)))))

(defn- resilient-embedder-for [entry collection-name content]
  (let [chain (some-> (:type entry)
                      (embed-svc/resolve-provider-chain-for-type+size content))]
    (if (seq chain)
      (resilient/resilient-embedder chain)
      (when-let [p (resolve-provider entry collection-name)]
        (resilient/resilient-embedder [{:provider p :provider-key :collection-fallback}])))))

(defrecord HiveMcpEmbedder []
  port/IEmbedder
  (-embed-entry [_ entry collection-name content]
    (if-let [embedder (resilient-embedder-for entry collection-name content)]
      (r/try-effect* :embedder/embed-failed
        ((requiring-resolve 'hive-mcp.chroma.embeddings/embed-text) embedder content))
      (r/err :embedder/no-provider
             {:type            (:type entry)
              :collection-name collection-name})))
  (-embed-text [_ collection-name text]
    (embed-svc/embed-for-collection collection-name text))
  (-routing-for-type [_ memory-type]
    (routing-map (embed-svc/resolve-provider-for-type memory-type)))
  (-routing-for-type+size [_ memory-type content]
    (routing-map (embed-svc/resolve-provider-for-type+size memory-type content)))
  (-no-embed-type? [_ memory-type]
    (embed-svc/no-embed-type? memory-type))
  (-collection-names [_]
    (embed-svc/type->collection-names nil))
  (-dimension-for-collection [_ collection-name]
    (embed-svc/get-dimension-for collection-name))
  (-configured-collection-names [_]
    (keys (embed-svc/list-configured-collections)))
  (-provider-available-for? [_ collection-name]
    (embed-svc/provider-available-for? collection-name))
  (-collection-backed? [_ collection-name]
    (embed-svc/collection-backed? collection-name)))

(defn install!
  "Install the hive-mcp-backed embedder into the port slot."
  []
  (port/set-embedder! (->HiveMcpEmbedder)))

(defn uninstall!
  []
  (port/reset-embedder!))