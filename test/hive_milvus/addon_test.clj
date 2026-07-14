(ns hive-milvus.addon-test
  (:require [clojure.test :refer [is use-fixtures]]
            [hive-mcp.addons.protocol :as addon-proto]
            [hive-mcp.protocols.memory :as mem-proto]
            [hive-milvus.addon :as addon]
            [hive-milvus.config :as config]
            [hive-milvus.embed.adapter :as embed-adapter]
            [hive-milvus.embed.port :as port]
            [hive-milvus.relocate-addon :as reloc-addon]
            [hive-milvus.store :as store]
            [hive-test.golden :as golden]
            [hive-test.mutation :as mut]
            [malli.core :as m]))

(def InitResult
  [:map
   [:success? :boolean]
   [:errors [:vector :string]]
   [:metadata [:map [:backend [:= "milvus"]]]]])

(def resolved-config
  {:host "localhost"
   :port 19530
   :collection-name "hive_mcp_memory"
   :token nil
   :database "default"
   :secure false
   :transport :http})

(use-fixtures
  :each
  (fn [test-fn]
    (port/reset-embedder!)
    (try
      (test-fn)
      (finally
        (port/reset-embedder!)))))

(defn run-initialization []
  (let [events (atom [])
        memory-store (Object.)
        instance (addon/create-addon)]
    (with-redefs [config/resolve-MilvusConfig (fn [_] {:ok resolved-config})
                  embed-adapter/install! #(swap! events conj :embed-installed)
                  store/create-store (fn [_]
                                       (swap! events conj :store-created)
                                       memory-store)
                  mem-proto/connect! (fn [actual _]
                                       (is (identical? memory-store actual))
                                       (swap! events conj :connected)
                                       {:success? true})
                  mem-proto/set-store! (fn [actual]
                                         (is (identical? memory-store actual))
                                         (swap! events conj :store-set)
                                         actual)
                  reloc-addon/install! #(swap! events conj :relocate-installed)]
      (let [result (addon-proto/initialize! instance {})]
        (is (m/validate InitResult result))
        {:result result
         :events @events
         :store-installed? (identical? memory-store @(:store-atom instance))
         :capabilities (addon-proto/capabilities instance)}))))

(golden/deftest-golden addon-initialization-lifecycle
  "test/golden/milvus/addon-initialization-lifecycle.edn"
  (run-initialization))

(mut/deftest-mutation-witness missing-embedder-install-is-caught
  hive-milvus.embed.adapter/install!
  (fn [] nil)
  (fn []
    (let [memory-store (Object.)
          instance (addon/create-addon)]
      (port/reset-embedder!)
      (with-redefs [config/resolve-MilvusConfig (fn [_] {:ok resolved-config})
                    store/create-store (fn [_] memory-store)
                    mem-proto/connect! (fn [_ _] {:success? true})
                    mem-proto/set-store! identity
                    reloc-addon/install! (fn [] nil)]
        (addon-proto/initialize! instance {})
        (is (instance? hive_milvus.embed.adapter.HiveMcpEmbedder
                       (port/current)))))))