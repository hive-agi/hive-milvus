(ns hive-milvus.embed.port-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [hive-test.mutation :as mut]
            [malli.core :as m]
            [hive-milvus.embed.adapter :as adapter]
            [hive-milvus.embed.port :as port]))

(use-fixtures
  :each
  (fn [test-fn]
    (port/reset-embedder!)
    (try
      (test-fn)
      (finally
        (port/reset-embedder!)))))

(deftest embedder-schema-rejects-non-adapters
  (is (m/validate port/Embedder (port/current)))
  (is (not (m/validate port/Embedder (Object.))))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Embedder must satisfy IEmbedder"
       (port/set-embedder! (Object.)))))

(deftest adapter-installation-satisfies-port
  (adapter/install!)
  (is (instance? hive_milvus.embed.adapter.HiveMcpEmbedder
                 (port/current)))
  (is (satisfies? port/IEmbedder (port/current)))
  (is (seq (port/collection-names))))

(deftest protocol-identity-survives-reload
  (adapter/install!)
  (let [adapter-instance (port/current)
        interface-before (:on-interface port/IEmbedder)]
    (require 'hive-milvus.embed.port :reload)
    (is (identical? interface-before (:on-interface port/IEmbedder)))
    (is (satisfies? port/IEmbedder adapter-instance))
    (is (boolean? (port/no-embed-type? :note)))))

(mut/deftest-mutation-witness installer-no-op-is-caught
  hive-milvus.embed.adapter/install!
  (fn [] nil)
  (fn []
    (port/reset-embedder!)
    (adapter/install!)
    (is (instance? hive_milvus.embed.adapter.HiveMcpEmbedder
                   (port/current)))))