(ns hive-milvus.store.guard
  "Size guard on the write path: a record whose VarChar field exceeds the
   collection schema's max_length is refused before it is embedded or sent,
   with a failure map that names the field, its size and the limit.

   Milvus counts a VarChar's max_length in UTF-8 bytes. Pure."
  (:require [malli.core :as m])
  (:import [java.nio.charset StandardCharsets]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(def varchar-limits
  "Record field -> max_length in bytes, as milvus-clj's memory schema
   declares them (`milvus-clj.schema/memory-schema-fields`)."
  {:id           256
   :document     65535
   :type         64
   :tags         4096
   :content      65535
   :content_hash 128
   :created      128
   :updated      128
   :duration     32
   :expires      128
   :project_id   256})

(defn utf8-bytes
  "UTF-8 byte length of S (nil counts as 0)."
  [s]
  (if (string? s)
    (alength (.getBytes ^String s StandardCharsets/UTF_8))
    0))

(defn oversize-fields
  "[{:field k :bytes n :limit max} ...] for every VarChar field of RECORD
   over its limit, in `varchar-limits` order. Empty when the record fits."
  [record]
  (into []
        (keep (fn [[k limit]]
                (let [n (utf8-bytes (get record k))]
                  (when (> n limit) {:field k :bytes n :limit limit}))))
        varchar-limits))

(defn refusal
  "The write failure for RECORD when it does not fit the schema, else nil.

     {:success? false :error :milvus/payload-too-large :reconnecting? false
      :id id :oversize [{:field :bytes :limit} ...] :errors [msg]}

   :reconnecting? false marks it permanent: a queue drain drops it rather
   than re-queueing a write that can never land."
  [record]
  (when-let [over (not-empty (oversize-fields record))]
    {:success?      false
     :error         :milvus/payload-too-large
     :reconnecting? false
     :id            (:id record)
     :oversize      over
     :errors        [(str "payload too large for milvus: "
                          (apply str
                                 (interpose ", "
                                            (map (fn [{:keys [field bytes limit]}]
                                                   (str (name field) " is " bytes
                                                        " bytes, limit " limit))
                                                 over))))]}))

(def Oversize
  [:map {:closed true}
   [:field :keyword]
   [:bytes nat-int?]
   [:limit pos-int?]])

(m/=> oversize-fields [:=> [:cat map?] [:vector Oversize]])
(m/=> refusal [:=> [:cat map?] [:maybe [:map
                                        [:success? [:= false]]
                                        [:error [:= :milvus/payload-too-large]]
                                        [:reconnecting? [:= false]]
                                        [:oversize [:vector Oversize]]
                                        [:errors [:vector :string]]]]])
