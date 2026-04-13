(ns hive-milvus.failure
  "Failure ADT + classifier + legacy-shape translator for hive-milvus.

   Effect-boundary pure core: classifies Throwables into a closed
   `MilvusFailure` ADT and translates ADT values back to the legacy
   `{:success? false :errors [...] :reconnecting? ...}` map shape that
   existing protocol callers expect.

   No side effects. No milvus-clj calls. Just shapes."
  (:require [clojure.string :as str]
            [hive-dsl.adt :as adt])
  (:import [io.grpc StatusRuntimeException]))

;; =============================================================================
;; ADT
;; =============================================================================

(adt/defadt MilvusFailure
  "Closed sum type of failure modes observed while talking to Milvus."
  [:milvus/transient         {:message string?}]
  [:milvus/fatal             {:message string?}]
  [:milvus/reconnect-timeout {:message string?}])

;; =============================================================================
;; Classifier (pure — no milvus/*, no I/O)
;; =============================================================================

(def ^:private transient-markers
  ["not connected"
   "UNAVAILABLE"
   "DEADLINE_EXCEEDED"
   "Keepalive failed"
   "connection is likely gone"])

(defn- transient-message?
  "True if `msg` contains any marker for a transient gRPC / connection drop."
  [msg]
  (boolean
    (when msg
      (let [s (str msg)]
        (some #(str/includes? s %) transient-markers)))))

(defn classify
  "Classify a Throwable into a MilvusFailure ADT value.

   - io.grpc.StatusRuntimeException        -> :milvus/transient
   - Exception whose message matches one
     of the transient markers               -> :milvus/transient
   - Anything else                          -> :milvus/fatal"
  [^Throwable t]
  (let [msg (or (some-> t .getMessage) "")]
    (if (or (instance? StatusRuntimeException t)
            (transient-message? msg))
      (milvus-failure :milvus/transient {:message msg})
      (milvus-failure :milvus/fatal     {:message msg}))))

(defn transient?
  "True if failure is a :milvus/transient variant."
  [failure]
  (= :milvus/transient (adt/adt-variant failure)))

(defn reconnect-timeout
  "Construct a :milvus/reconnect-timeout failure with the given message."
  [message]
  (milvus-failure :milvus/reconnect-timeout {:message (or message "")}))

;; =============================================================================
;; Legacy shape translator
;; =============================================================================

(defn ->legacy-map
  "Translate a MilvusFailure ADT to the legacy fail-map shape expected by
   hive-mcp protocol callers:

     {:success? false :errors [msg] :reconnecting? bool}

   :reconnecting? is true for transient and reconnect-timeout variants
   (the background heal loop may still recover), false for fatal."
  [failure]
  (let [msg     (:message failure)
        variant (adt/adt-variant failure)]
    {:success?      false
     :errors        [msg]
     :reconnecting? (not= :milvus/fatal variant)}))
