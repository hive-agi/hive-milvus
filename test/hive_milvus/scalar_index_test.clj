(ns hive-milvus.scalar-index-test
  (:require [clojure.test :refer [deftest is]]
            [hive-milvus.store.rest-index :as rest]
            [hive-milvus.store.index :as index]))

(deftest preserves-existing-field-indexes
  (let [calls (atom [])
        request (fn [path body]
                  (swap! calls conj [path body])
                  (case path
                    "/v2/vectordb/indexes/list" ["custom_type_name"]
                    "/v2/vectordb/indexes/describe" [{:fieldName "type" :indexState "Finished"}]
                    "/v2/vectordb/indexes/create" {}))]
    (rest/ensure-fields! request "scratch" ["type" "project_id"])
    (is (= {:collectionName "scratch"
            :indexParams [{:fieldName "project_id" :indexName "project_id_idx"
                           :indexType "INVERTED"}]}
           (second (last @calls))))
    (is (= 3 (count @calls)))))

(deftest partial-failure-retries-without-memoizing
  (let [memo (atom #{}) calls (atom 0)
        install (fn [] (when (= 1 (swap! calls inc))
                         (throw (ex-info "refused" {}))))]
    (is (false? (index/ensure-indexed! memo "scratch" install)))
    (is (empty? @memo))
    (is (true? (index/ensure-indexed! memo "scratch" install)))
    (index/ensure-indexed! memo "scratch" install)
    (is (= 2 @calls))
    (is (= #{"scratch"} @memo))))

(deftest failed-server-build-is-not-success
  (is (thrown? clojure.lang.ExceptionInfo
        (rest/ensure-fields!
          (fn [path _]
            (if (= path "/v2/vectordb/indexes/list") ["type_idx"]
                [{:fieldName "type" :indexState "Failed" :failReason "bad"}]))
          "scratch" ["type" "project_id"]))))

(deftest transport-failure-propagates
  (is (thrown? clojure.lang.ExceptionInfo
        (rest/ensure-fields! (fn [& _] (throw (ex-info "offline" {})))
                            "scratch" ["type"]))))

(deftest checked-http-boundary
  (let [response (atom "{\"code\":0,\"data\":{}}")
        seen (atom [])
        server (com.sun.net.httpserver.HttpServer/create
                 (java.net.InetSocketAddress. "127.0.0.1" 0) 0)]
    (.createContext server "/v2/vectordb/indexes/create"
      (reify com.sun.net.httpserver.HttpHandler
        (handle [_ exchange]
          (swap! seen conj
            {:body (slurp (.getRequestBody exchange))
             :auth (.getFirst (.getRequestHeaders exchange) "Authorization")})
          (let [body (.getBytes @response java.nio.charset.StandardCharsets/UTF_8)]
            (.sendResponseHeaders exchange 200 (alength body))
            (with-open [out (.getResponseBody exchange)] (.write out body))))))
    (.start server)
    (try
      (let [config {:host "127.0.0.1" :port (.getPort (.getAddress server))
                    :token "test-only" :database "test-db"}]
        (is (= {} (rest/post! config "/v2/vectordb/indexes/create"
                              {:collectionName "scratch"})))
        (is (= "Bearer test-only" (:auth (first @seen))))
        (is (re-find #"test-db" (:body (first @seen))))
        (reset! response "{\"code\":1100,\"message\":\"bad index\"}")
        (is (thrown? clojure.lang.ExceptionInfo
              (rest/post! config "/v2/vectordb/indexes/create" {}))))
      (finally (.stop server 0)))))

(deftest pending-build-reprobes-without-recreating
  (let [calls (atom [])
        request (fn [path _]
                  (swap! calls conj path)
                  (case path
                    "/v2/vectordb/indexes/list" ["type_idx" "project_idx"]
                    "/v2/vectordb/indexes/describe"
                    [{:fieldName "type" :indexState "InProgress"}
                     {:fieldName "project_id" :indexState "Finished"}]))
        memo (atom #{})]
    (is (false? (index/ensure-indexed! memo "scratch"
                  #(rest/ensure-fields! request "scratch" ["type" "project_id"]))))
    (is (empty? @memo))
    (is (not-any? #{"/v2/vectordb/indexes/create"} @calls))))

(deftest finished-builds-are-idempotent
  (let [memo (atom #{}) calls (atom 0)
        request (fn [path _]
                  (swap! calls inc)
                  (if (= path "/v2/vectordb/indexes/list") ["scalar"]
                      [{:fieldName "type" :indexState "Finished"}
                       {:fieldName "project_id" :indexState "Finished"}]))]
    (dotimes [_ 2]
      (index/ensure-indexed! memo "scratch"
        #(rest/ensure-fields! request "scratch" ["type" "project_id"])))
    (is (= #{"scratch"} @memo))
    (is (= 2 @calls))))
