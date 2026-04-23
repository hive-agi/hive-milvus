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

   The classifier walks the `.getCause` chain, not just the top
   throwable. This matters because `milvus-clj.api/*` returns
   `future`s that hive-milvus derefs inside the resilient body; a
   transport-thrown `ex-info` carrying the `::client/transport` tag
   re-surfaces wrapped in `java.util.concurrent.ExecutionException`,
   whose own ex-data is empty. Classifying only the top wrapper would
   drop every such transient into `:milvus/fatal` and starve the heal
   loop — the exact failure mode that motivated
   `hive-mcp.vectordb.resilience/transient-failure?`.

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
