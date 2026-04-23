(ns hive-milvus.config
  "Typed config for Milvus addon — declared via hive-di defconfig.

   Replaces ad-hoc env-var parsing previously scattered through addon.clj.
   Generates (via the macro):

     MilvusConfig             — ADT (:config/resolved | :unresolved | :invalid)
     MilvusConfig-fields      — field registry (source of truth)
     MilvusConfig-schema      — Malli closed map schema
     resolve-MilvusConfig     — resolver fn (0/1/2 arity) returning Result

   Resolution order (per field):
     1. Explicit overrides map (e.g. addon manifest config)
     2. Environment variable lookup (System/getenv by default)
     3. blank->nil normalization (\"\" → trigger default)
     4. Pre-typed default (skips coercion)
     5. hive-dsl.coerce on string env values

   Optional fields (currently :token) resolve to nil when unset."
  (:require [hive-di.core :refer [defconfig env]]))

(defconfig MilvusConfig
  :host             (env "MILVUS_HOST"
                         :default "localhost"
                         :type :string
                         :doc "Milvus host (gRPC or HTTP gateway)")
  :port             (env "MILVUS_PORT"
                         :default 19530
                         :type :int
                         :doc "Milvus port (gRPC default 19530)")
  :transport        (env "MILVUS_TRANSPORT"
                         :default :grpc
                         :type :keyword
                         :doc "Transport keyword: :grpc or :http")
  :token            (env "MILVUS_TOKEN"
                         :type :string
                         :required false
                         :doc "Auth token (Zilliz Cloud / RBAC). Optional.")
  :database         (env "MILVUS_DATABASE"
                         :default "default"
                         :type :string
                         :doc "Milvus database name")
  :secure           (env "MILVUS_SECURE"
                         :default false
                         :type :bool
                         :doc "Use TLS/SSL for the connection")
  ;; collection-name is declared as env-sourced for symmetry — addon
  ;; manifests may override it explicitly. The actual env var
  ;; MILVUS_COLLECTION_NAME is rarely set; the default carries the load.
  :collection-name  (env "MILVUS_COLLECTION_NAME"
                         :default "hive_mcp_memory"
                         :type :string
                         :doc "Target Milvus collection name"))
