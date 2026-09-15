(ns hive-milvus.dial
  "The one place the Milvus transport is opened.

   Every dial goes through `dial!` so that a refused connection names the
   address it was refused by. A generic work queue upstream runs an opaque
   thunk and cannot learn the endpoint, so the endpoint is attached here, at
   the only layer that holds it."
  (:require [milvus-clj.api :as milvus]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(defn endpoint-str
  "\"host:port\" for CFG."
  [{:keys [host port]}]
  (str host ":" port))

(defn dial!
  "Open the Milvus transport for CFG. Returns what `milvus-clj.api/connect!`
   returns.

   Throws `ex-info` naming the endpoint, with ex-data
   {:type :milvus/dial-failed :endpoint \"host:port\" :host h :port p} and the
   transport throwable as its CAUSE, so a report built from a cause chain
   carries the address without being told it."
  [cfg]
  (try
    (milvus/connect! cfg)
    (catch Throwable t
      (throw (ex-info (str "milvus dial failed: " (endpoint-str cfg))
                      {:type     :milvus/dial-failed
                       :endpoint (endpoint-str cfg)
                       :host     (:host cfg)
                       :port     (:port cfg)}
                      t)))))
