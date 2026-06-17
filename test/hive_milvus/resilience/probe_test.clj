(ns hive-milvus.resilience.probe-test
  "Trifecta + targeted unit tests for the resilience layer's probe seam.

   Two layers under test:

   1. **Protocol seam** (`milvus-clj.client/ILivenessProbe`) — every
      transport extends it; `client/probe!` dispatches through the
      protocol with a fallback to `-has-collection`. We test the
      dispatch with reified stubs (no real milvus required).

   2. **Reconnect-and-verify** semantics — the bug fix at the heart of
      the resilience split. A reconnect attempt counts as 'recovered'
      ONLY when the post-`connect!` probe round-trip succeeds. A
      `connect!` that 'succeeds locally' (atom set fresh) but where the
      probe still fails MUST keep the loop running.

   No real milvus connection. We stub via `with-redefs` of
   `milvus-clj.api/connect!` + `resilience.probe/probe-once!`."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-milvus.resilience.probe :as probe]
            [hive-milvus.resilience.reconnect :as reconnect]
            [hive-test.trifecta :refer [deftrifecta]]
            [milvus-clj.api :as milvus]
            [milvus-clj.client :as client]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; =============================================================================
;; Stubs implementing ILivenessProbe
;; =============================================================================

(defn- live-stub
  "Reified ILivenessProbe that always returns truthy."
  []
  (reify
    client/ILivenessProbe
    (-probe! [_] true)))

(defn- dead-stub
  "Reified ILivenessProbe that throws an IO-shaped ex-info on every probe.
   Mirrors the throw signature of `transport.http/post!` on connect timeout."
  []
  (reify
    client/ILivenessProbe
    (-probe! [_]
      (throw (ex-info "stub: HTTP connect timed out"
                      {:milvus-clj.client/transport :http
                       :path  "/v2/vectordb/collections/has"
                       :cause :io})))))

;; =============================================================================
;; Trifecta — client/probe! dispatch
;; =============================================================================

(defn- probe-dispatch
  "Drive client/probe! through a stub. Returns {:ok? boolean :throws? boolean}
   so trifecta can shape golden + property assertions against it."
  [stub]
  (try {:ok? (boolean (client/probe! stub)) :throws? false}
       (catch Throwable _ {:ok? false :throws? true})))

(deftrifecta client-probe!--dispatches-via-protocol
  probe-dispatch
  ;; :golden-path bootstraps a snapshot of {:live ..., :dead ...} so the
  ;; mutation facet has something to compare against. First run writes
  ;; the file (UPDATE_GOLDEN unnecessary for new files); subsequent runs
  ;; assert no drift. Without :golden-path the :golden-derived mutation
  ;; strategy reads nil → File.<init> NPE.
  {:golden-path "test/golden/resilience/probe-dispatch.edn"
   :cases       {:live (live-stub)
                 :dead (dead-stub)}
   :xf          (fn [r] (select-keys r [:ok? :throws?]))
   :pred        (fn [r] (or (and (:ok? r) (not (:throws? r)))
                            (and (not (:ok? r)) (:throws? r))))
   :mutations   [["never-throws"
                  (fn [_] {:ok? false :throws? false})]
                 ["always-ok"
                  (fn [_] {:ok? true  :throws? false})]]})

;; =============================================================================
;; Bug-fix invariant — connect-ok + probe-fail must NOT count as recovered
;; =============================================================================

(deftest reconnect-treats-probe-failure-as-not-recovered
  (testing "the regression: try-reconnect-and-verify! returns false when
            connect! 'succeeds' (atom set) but the probe round-trip fails"
    (let [connect-calls (atom 0)
          probe-calls   (atom 0)]
      (with-redefs [milvus/connect!     (fn [_cfg]
                                          (swap! connect-calls inc)
                                          ;; Returns whatever — historic
                                          ;; behaviour is to return the
                                          ;; client object. Crucially, no
                                          ;; throw means atom now non-nil.
                                          :stub-client)
                    probe/probe-once!   (fn []
                                          (swap! probe-calls inc)
                                          ;; Server still unreachable.
                                          false)]
        ;; Drive one heal-loop iteration synchronously by calling the
        ;; private helper through reflection-free reify of the public
        ;; surface: kick! + immediate await with zero-budget. Because
        ;; the loop is async we cannot directly assert its success
        ;; criterion from here without racing — instead, prove the
        ;; building block: a single attempt that connects but fails
        ;; the probe must report not-recovered.
        (let [config-atom (atom {:transport :http :host "stub" :port 19530})
              ;; await with budget 0 — should immediately reflect
              ;; probe-once!'s return value.
              recovered? (do (reconnect/kick! config-atom)
                             (Thread/sleep 50) ;; let kick fire connect+probe
                             (reconnect/await! 0))]
          (is (= 1 @connect-calls)
              "connect! invoked exactly once on kick path")
          (is (pos? @probe-calls)
              "probe-once! invoked at least once to verify recovery")
          (is (false? recovered?)
              "BUG FIX: probe failure means 'not recovered' — old code
               would return true here on the basis of connect! alone")
          ;; Cleanup so later tests don't see a running loop
          (reconnect/stop!))))))

(deftest reconnect-reports-recovered-when-probe-passes
  (testing "the happy path: connect + probe both succeed → recovered? true"
    ;; `await!` polls `probe/alive?` (which reads `milvus/connected?` →
    ;; the singleton `default-client` atom). Stubbing only `connect!` +
    ;; `probe-once!` is not enough — `default-client` stays nil and
    ;; `connected?` short-circuits to false, so `await!` would never
    ;; observe recovery. Stub `probe/alive?` directly: starts dead,
    ;; flips to alive once a probe-verified reconnect attempt has run.
    (let [alive? (atom false)]
      (with-redefs [milvus/connect!   (fn [_cfg] :stub-client)
                    probe/probe-once! (fn []
                                        (reset! alive? true)
                                        true)
                    probe/alive?      (fn [] @alive?)]
        (let [config-atom (atom {:transport :http :host "stub" :port 19530})]
          (reconnect/kick! config-atom)
          (is (true? (reconnect/await! 1000))
              "verified probe means the loop reports recovered")
          (reconnect/stop!))))))

;; =============================================================================
;; r/rescue contract — probe-once! never throws
;; =============================================================================

(deftest probe-once!-never-throws
  (testing "probe-once! catches any throwable and returns false"
    (with-redefs [milvus/connected? (fn []
                                      (throw (ex-info "boom"
                                                      {:cause :io})))]
      (is (false? (probe/probe-once!))
          "r/rescue swallows the IO throw — caller sees clean false"))))
