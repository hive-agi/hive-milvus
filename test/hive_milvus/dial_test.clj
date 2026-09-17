(ns hive-milvus.dial-test
  "A refused dial has to name what it was dialling. The class and message of a
   raw java.net.ConnectException carry no address, so an upstream report built
   from class + message + cause chain cannot recover one."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-milvus.dial :as dial]
            [milvus-clj.api :as milvus]))

(def ^:private cfg {:host "10.104.172.142" :port 19530})

(defn- dial-failure
  "The throwable `dial!` raises when the transport refuses, or nil."
  [thrown]
  (with-redefs [milvus/connect! (fn [_] (throw thrown))]
    (try (dial/dial! cfg) nil
         (catch Throwable t t))))

(deftest a-refused-dial-names-the-endpoint
  (let [refused (java.net.ConnectException. "Connection refused")
        t       (dial-failure refused)]
    (testing "the failure is an ex-info carrying the endpoint as data"
      (is (instance? clojure.lang.ExceptionInfo t))
      (is (= "10.104.172.142:19530" (:endpoint (ex-data t))))
      (is (= "10.104.172.142" (:host (ex-data t))))
      (is (= 19530 (:port (ex-data t))))
      (is (= :milvus/dial-failed (:type (ex-data t)))))

    (testing "and in the message, which is what a plain log line shows"
      (is (re-find #"10\.104\.172\.142:19530" (ex-message t))))

    (testing "the original throwable is the cause, not replaced by the wrapper"
      (is (identical? refused (ex-cause t)))
      (is (= "Connection refused" (ex-message (ex-cause t)))))))

(deftest a-successful-dial-is-passed-through-untouched
  (with-redefs [milvus/connect! (fn [c] [:connected c])]
    (is (= [:connected cfg] (dial/dial! cfg)))))

(deftest endpoint-str-reads-the-config
  (is (= "localhost:19530" (dial/endpoint-str {:host "localhost" :port 19530})))
  (is (= "milvus.svc:19531" (dial/endpoint-str {:host "milvus.svc" :port 19531}))))
