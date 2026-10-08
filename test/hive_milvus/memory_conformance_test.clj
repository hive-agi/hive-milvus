(ns ^:live hive-milvus.memory-conformance-test
  "The whole hive-spi memory conformance suite against a LIVE MilvusMemoryStore.

   ^:live: CI runs `--skip-meta :live`. Opt-in locally, and measured: the
   cases run only when MILVUS_CONFORMANCE_HOST names a Milvus instance (port
   from MILVUS_CONFORMANCE_PORT, default 19530) that accepts a TCP connect
   within one second. It never falls back to MILVUS_HOST or MilvusConfig:
   reset-store! drops every collection the store knows, so point it only at
   an instance dedicated to this suite. Unset or unreachable, every case is
   skipped with a printed reason.

   The write contract alone is also checked without a backend, over the fake
   client port, in `hive-milvus.store.write-contract-test`."
  (:require [clojure.test :refer [use-fixtures]]
            [hive-milvus.store :as store]
            [hive-spi.memory.conformance :as conformance]
            [hive-spi.memory.ports :as ports])
  (:import [java.net InetSocketAddress Socket]))

(defn- host [] (System/getenv "MILVUS_CONFORMANCE_HOST"))

(defn- port [] (parse-long (or (System/getenv "MILVUS_CONFORMANCE_PORT") "19530")))

(defn reachable?
  "True when HOST names a host and HOST:PORT accepts a TCP connection within
   one second."
  [host port]
  (boolean
   (when (seq host)
     (try
       (with-open [s (Socket.)]
         (.connect s (InetSocketAddress. ^String host (int port)) 1000)
         true)
       (catch Exception _ false)))))

(defonce ^:private store-atom (atom nil))

(defn- connect-config []
  {:host (host) :port (port) :collection-name "hive_conformance_test"})

(defn- make-store
  "One connected store per namespace run; the suite resets it per case."
  []
  (or (when-let [s @store-atom] (when (ports/connected? s) s))
      (let [s (store/create-store (connect-config))
            r (ports/connect! s (connect-config))]
        (when-not (:success? r)
          (throw (ex-info "milvus connect! failed" (select-keys r [:errors :error]))))
        (reset! store-atom s)
        s)))

(def ^:private live? (delay (reachable? (host) (port))))

(use-fixtures :once
  (fn [f]
    (if @live?
      (try (f)
           (finally
             (when-let [s @store-atom]
               (ports/disconnect! s)
               (reset! store-atom nil))))
      (println "hive-milvus memory conformance: MILVUS_CONFORMANCE_HOST unset or unreachable"
               (pr-str (host)) "- suite skipped"))))

(conformance/defconformance milvus make-store {:connect-config (connect-config)})
