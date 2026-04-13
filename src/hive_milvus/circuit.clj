(ns hive-milvus.circuit
  "Circuit breaker for Milvus protocol operations.

   States: :closed (normal) | :open (fail-fast) | :half-open (probing).

   Flow:
   - :closed - calls pass through. N consecutive connection failures
     (threshold) -> :open with opened-at=now.
   - :open - `check` returns {:success? false :error :circuit-open
     :retry-after ms :reconnecting? true} until cooldown-ms has elapsed
     since opened-at.
   - cooldown elapsed - next `check` transitions to :half-open and allows
     the call through as a probe. `record-success!` closes the breaker
     and notifies subscribers; `record-failure!` reopens it (resets
     opened-at=now).

   Subscribers: callers can `subscribe!` a 0-arity fn invoked on every
   :closed transition (open->closed or half-open->closed). Used by the
   queue drain module to flush buffered writes when Milvus is healthy.

   Thread-safety: single atom, swap! transitions; listener fns run on
   the caller's thread that closed the breaker (keep them fast or spawn
   futures inside)."
  (:require [taoensso.timbre :as log]))

;; =========================================================================
;; State
;; =========================================================================

(def ^:private default-state
  {:state       :closed
   :failures    0
   :opened-at   nil
   :threshold   5
   :cooldown-ms 15000})

(def ^:private breaker-state (atom default-state))

(def ^:private listeners (atom #{}))

;; =========================================================================
;; Configuration
;; =========================================================================

(defn configure!
  "Update breaker tunables without changing :state. Accepts
   {:threshold N :cooldown-ms ms} - only non-nil keys are merged."
  [{:keys [threshold cooldown-ms]}]
  (swap! breaker-state
         (fn [s]
           (cond-> s
             (some? threshold)   (assoc :threshold threshold)
             (some? cooldown-ms) (assoc :cooldown-ms cooldown-ms))))
  nil)

(defn state
  "Return the current breaker state map."
  []
  @breaker-state)

(defn open?       [] (= :open       (:state @breaker-state)))
(defn closed?     [] (= :closed     (:state @breaker-state)))
(defn half-open?  [] (= :half-open  (:state @breaker-state)))

;; =========================================================================
;; Listeners
;; =========================================================================

(defn subscribe!
  "Register a 0-arity fn to be invoked on every transition to :closed.
   Returns the registered fn. Duplicate registrations are no-ops."
  [f]
  (swap! listeners conj f)
  f)

(defn unsubscribe!
  "Deregister a previously-subscribed listener fn."
  [f]
  (swap! listeners disj f)
  f)

(defn- notify-closed!
  "Invoke all registered listeners. Exceptions are logged and swallowed
   so a broken listener cannot block the close transition."
  []
  (doseq [f @listeners]
    (try
      (f)
      (catch Throwable t
        (log/warn t "circuit listener threw on :closed transition")))))

;; =========================================================================
;; Check / record
;; =========================================================================

(defn check
  "Gate a protocol call against the breaker.

   Returns nil when the call is allowed (:closed or :half-open probe).
   Returns a fail map when :open and still in cooldown:

     {:success?      false
      :error         :circuit-open
      :retry-after   <ms remaining>
      :reconnecting? true}

   Side effect: transitions :open -> :half-open when cooldown has
   elapsed, so the next caller gets a probe slot."
  []
  (let [now   (System/currentTimeMillis)
        new-s (swap! breaker-state
                       (fn [{:keys [state opened-at cooldown-ms] :as s}]
                         (if (and (= state :open)
                                  opened-at
                                  (>= (- now opened-at) cooldown-ms))
                           (assoc s :state :half-open)
                           s)))]
    (case (:state new-s)
      :closed     nil
      :half-open  nil
      :open       (let [remaining (max 0 (- (:cooldown-ms new-s)
                                             (- now (:opened-at new-s))))]
                    {:success?      false
                     :error         :circuit-open
                     :retry-after   remaining
                     :reconnecting? true}))))

(defn record-failure!
  "Record a connection-failure outcome. State transitions:

   - :closed    -> inc failures; if failures>=threshold trip :open.
   - :half-open -> trip back to :open (probe failed).
   - :open      -> no-op (cooldown already running)."
  []
  (let [now (System/currentTimeMillis)
        prev (swap-vals! breaker-state
                         (fn [{:keys [state failures threshold] :as s}]
                           (case state
                             :closed
                             (let [f' (inc (or failures 0))]
                               (if (>= f' (or threshold 5))
                                 (assoc s :state :open :failures f' :opened-at now)
                                 (assoc s :failures f')))

                             :half-open
                             (assoc s :state :open
                                      :opened-at now
                                      :failures  (or threshold 5))

                             :open
                             s)))]
    (let [before (first prev)
          after  (second prev)]
      (when (and (not= :open (:state before)) (= :open (:state after)))
        (log/warn "Milvus circuit OPENED after"
                  (:failures after) "failures; cooldown"
                  (:cooldown-ms after) "ms")))
    nil))

(defn record-success!
  "Record a successful outcome. State transitions:

   - :closed    -> reset failures to 0.
   - :half-open -> close the breaker, reset failures, notify listeners.
   - :open      -> close the breaker, reset failures, notify listeners
                   (defensive: should not normally happen because :open
                   short-circuits via `check`)."
  []
  (let [[before after]
        (swap-vals! breaker-state
                    (fn [{:keys [state] :as s}]
                      (case state
                        :closed    (assoc s :failures 0)
                        :half-open (assoc s :state :closed :failures 0 :opened-at nil)
                        :open      (assoc s :state :closed :failures 0 :opened-at nil))))]
    (when (and (not= :closed (:state before)) (= :closed (:state after)))
      (log/info "Milvus circuit CLOSED (recovered from" (:state before) ")")
      (notify-closed!))
    nil))

(defn force-reset!
  "Force the breaker back to fresh :closed state. Intended for tests
   and manual admin recovery. Does NOT notify listeners."
  []
  (reset! breaker-state default-state)
  nil)
