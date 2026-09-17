(ns hive-milvus.embed.adapter-host-seam-test
  "The adapter's soft targets must EXIST on the host it is paired with.

   A defsoft whose target vanished fails only at the first write, as the
   live coordinator did on 2026-09-17 when hive-mcp moved
   hive-mcp.chroma.embeddings to hive-mcp.embeddings.active and every
   memory add answered \"embed-for-entry failed\". This suite runs with the
   test-scoped hive-mcp on the classpath and pins that each seam resolves,
   embed-text through at least one of its two homes."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-addon.host :as host]))

(def service-targets
  '[hive-mcp.embeddings.service/resolve-provider-for-type
    hive-mcp.embeddings.service/resolve-provider-for-type+size
    hive-mcp.embeddings.service/resolve-provider-chain-for-type+size
    hive-mcp.embeddings.service/get-provider-for
    hive-mcp.embeddings.service/type->collection-names
    hive-mcp.embeddings.service/get-dimension-for
    hive-mcp.embeddings.service/list-configured-collections
    hive-mcp.embeddings.service/provider-available-for?
    hive-mcp.embeddings.service/collection-backed?
    hive-mcp.embeddings.service/embed-for-collection
    hive-mcp.embeddings.resilient/resilient-embedder])

(deftest every-service-seam-resolves-on-the-paired-host
  (doseq [sym service-targets]
    (is (host/available? sym) (str sym " must exist on the host"))))

(deftest embed-text-has-a-home-on-the-paired-host
  (testing "the moved home or the legacy one, never neither"
    (is (or (host/available? 'hive-mcp.embeddings.active/embed-text)
            (host/available? 'hive-mcp.chroma.embeddings/embed-text)))))
