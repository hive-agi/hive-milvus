(ns hive-milvus.failure
  "Failure ADT + classifier + legacy-shape translator for hive-milvus.

   Effect-boundary pure core: classifies Throwables into a closed
   `MilvusFailure` ADT and translates ADT values back to the legacy
   `{:success? false :errors [...] :reconnecting? ...}` map shape that
   existing protocol callers expect.

   Classification delegates to `milvus-clj.client/classify-error`, which
   dispatches on the `::client/transport` ex-data tag set by each
   transport. This means HTTP-thrown failures (IOException, HTTP 5xx)
   are recognized just as well as gRPC StatusRuntimeException ones —
   essential for PR-3's hybrid-transport story where the same store.clj
   resilience machinery serves both.

   No side effects. No milvus/* singleton calls. Just shapes."
  (:require [clojure.string :as str]
            [hive-dsl.adt :as adt]
            [milvus-clj.client :as client]))

;; =============================================================================
;; ADT
;; =============================================================================

(adt/defadt MilvusFailure
  "Closed sum type of failure modes observed while talking to Milvus."
  [:milvus/transient         {:message string?}]
  [:milvus/fatal             {:message string?}]
  [:milvus/reconnect-timeout {:message string?}])

;; =============================================================================
;; Classifier (pure — no I/O)
;; =============================================================================

(def ^:private transient-markers
  "Fallback markers for non-tagged exceptions (legacy gRPC paths that
   don't go through the new `client/classify-error` dispatch). The
   primary path uses the ::client/transport ex-data tag, which is set
   by both `transport.grpc` and `transport.http`."
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

   Primary path: delegate to `milvus-clj.client/classify-error`, which
   dispatches on the transport-tagged ex-data and returns
   `:connection-failure | :retryable | :fatal`. Both `:connection-failure`
   and `:retryable` map to `:milvus/transient` (the resilient retry path
   handles both the same way); `:fatal` maps to `:milvus/fatal`.

   Fallback path (for legacy untagged exceptions): match the throwable's
   message against `transient-markers`. Preserved so that pre-PR-3 code
   paths still classify correctly during the migration.

   Returns a `MilvusFailure` ADT value, never throws."
  [^Throwable t]
  (let [msg (or (some-> t .getMessage) "")
        category (try (client/classify-error t)
                      (catch Throwable _ nil))]
    (cond
      (= :fatal category)
      (milvus-failure :milvus/fatal {:message msg})

      (or (= :connection-failure category) (= :retryable category))
      (milvus-failure :milvus/transient {:message msg})

      ;; Fallback: legacy marker matching for untagged exceptions.
      (transient-message? msg)
      (milvus-failure :milvus/transient {:message msg})

      :else
      (milvus-failure :milvus/fatal {:message msg}))))

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
