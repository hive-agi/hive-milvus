(ns hive-milvus.failure
  "Failure ADT + classifier + legacy-shape translator for hive-milvus.

   Pure, no side effects: classifies Throwables into the closed
   `MilvusFailure` ADT and translates ADT values back to the legacy
   `{:success? false :errors [...] :reconnecting? ...}` map shape that
   protocol callers expect. Classification delegates to
   `milvus-clj.client/classify-error` (dispatch on the
   `::client/transport` ex-data tag) and walks the full `.getCause`
   chain, not just the top throwable."
  (:require [clojure.string :as str]
            [hive-dsl.adt :as adt]
            [milvus-clj.client :as client]
            [malli.core :as m]))

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
   don't go through the new `client/classify-error` dispatch, and
   ExecutionException wrappers whose ex-data is lost on the wrap).
   The primary path uses the ::client/transport ex-data tag, which is
   set by both `transport.grpc` and `transport.http`."
  ["not connected"
   "UNAVAILABLE"
   "DEADLINE_EXCEEDED"
   "Keepalive failed"
   "connection is likely gone"
   "selector manager closed"
   "IO failure"
   "Connection reset"
   "Broken pipe"])

(defn- transient-message?
  "True if `msg` contains any marker for a transient gRPC / connection drop."
  [msg]
  (boolean
    (when msg
      (let [s (str msg)]
        (some #(str/includes? s %) transient-markers)))))

(def ^:private ^:const max-cause-depth
  "Hard cap on `.getCause` chain traversal. Real JVM chains rarely
   exceed 5 links (ExecutionException -> RuntimeException ->
   StatusRuntimeException is the common worst case); 10 is generous
   headroom without letting a pathological chain stall the classifier."
  10)

(defn- causal-chain
  "Seq of `t` and every `.getCause` link, stopping on nil, a self-cycle,
   or `max-cause-depth` links — whichever comes first. Depth cap is a
   belt-and-braces guard for pathological wrappers beyond the
   identity-cycle check."
  [^Throwable t]
  (loop [t t acc []]
    (if (or (nil? t)
            (>= (count acc) max-cause-depth)
            (some #(identical? t %) acc))
      acc
      (recur (.getCause t) (conj acc t)))))

(defn- link-transient?
  "True if a single throwable link classifies as transient via the
   transport-tagged `client/classify-error` dispatch OR via the
   legacy message-marker fallback. Per-link so `classify` can walk
   the cause chain and match on the tagged inner throwable even when
   an outer wrapper (ExecutionException, RuntimeException, ...) has
   dropped the ex-data."
  [^Throwable link]
  (let [category (try (client/classify-error link)
                      (catch Throwable _ nil))]
    (or (= :connection-failure category)
        (= :retryable category)
        (boolean (:milvus/timeout (ex-data link)))
        (transient-message? (some-> link .getMessage)))))

(defn classify
  "Classify a Throwable into a MilvusFailure ADT value.

   Walks the `.getCause` chain and returns `:milvus/transient` when
   ANY link classifies as transient — either through the
   `::client/transport`-tagged `milvus-clj.client/classify-error`
   dispatch (`:connection-failure` / `:retryable`) or through the
   legacy message-marker fallback. Otherwise `:milvus/fatal`.

   Walking the chain matters because `milvus-clj.api/*` returns
   `future`s; when hive-milvus derefs them, a transport-thrown
   `ex-info` tagged `{::client/transport :http :cause :io}` resurfaces
   wrapped in `java.util.concurrent.ExecutionException`, whose own
   ex-data is empty. Only by inspecting the cause do we see the tag
   and route the failure to the heal loop instead of re-throwing.

   The resulting ADT value's `:message` preserves the top throwable's
   message for legacy-shape continuity. Returns a `MilvusFailure`,
   never throws."
  [^Throwable t]
  (let [msg   (or (some-> t .getMessage) "")
        chain (causal-chain t)]
    (if (some link-transient? chain)
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
   protocol callers:

     {:success? false :errors [msg] :reconnecting? bool}

   :reconnecting? is true for transient and reconnect-timeout variants,
   false for fatal."
  [failure]
  (let [msg     (:message failure)
        variant (adt/adt-variant failure)]
    {:success?      false
     :errors        [msg]
     :reconnecting? (not= :milvus/fatal variant)}))

(def MilvusFailureSchema
  "Runtime shape of the MilvusFailure ADT value returned by classify."
  [:map {:closed false}
   [:adt/type [:= :MilvusFailure]]
   [:adt/variant [:enum :milvus/transient :milvus/fatal]]
   [:message :string]])

(m/=> classify [:=> [:cat [:fn #(instance? Throwable %)]] MilvusFailureSchema])

(def AnyMilvusFailureSchema
  "Runtime shape of any MilvusFailure ADT value, covering all three variants."
  [:map {:closed false}
   [:adt/type [:= :MilvusFailure]]
   [:adt/variant [:enum :milvus/transient :milvus/fatal :milvus/reconnect-timeout]]
   [:message :string]])

(def ReconnectTimeoutFailureSchema
  "Runtime shape of the :milvus/reconnect-timeout MilvusFailure variant."
  [:map {:closed false}
   [:adt/type [:= :MilvusFailure]]
   [:adt/variant [:= :milvus/reconnect-timeout]]
   [:message :string]])

(def LegacyFailMapSchema
  "Legacy fail-map shape returned to protocol callers:
   {:success? false :errors [msg] :reconnecting? bool}."
  [:map {:closed false}
   [:success? [:= false]]
   [:errors [:tuple :string]]
   [:reconnecting? :boolean]])

(m/=> transient? [:=> [:cat AnyMilvusFailureSchema] :boolean])

(m/=> reconnect-timeout [:=> [:cat [:maybe :string]] ReconnectTimeoutFailureSchema])

(m/=> ->legacy-map [:=> [:cat AnyMilvusFailureSchema] LegacyFailMapSchema])
