(ns hive-milvus.resilience.probe
  "Liveness probing — Boundary stage of the resilience CPPB pipeline.

   Single responsibility: ask the underlying milvus client whether it can
   actually reach the server, with a tiny TTL cache so a burst of probes
   amortises to one RPC.

   Transport-agnostic: dispatches through `milvus-clj.client/ILivenessProbe`
   (extended by every transport's defrecord). Adding a new transport
   requires zero changes here — that is the point of the protocol seam.

   Why a separate ns from reconnect/retry: each of probe / reconnect /
   retry has its own rate of change. The probe surface is the most stable
   (one cached read); reconnect-loop algorithm and retry budget knobs
   change more often. Splitting them keeps each ns small and individually
   testable (mock the protocol; no need to mock a loop).

   Time discipline: never call `System/currentTimeMillis` directly — go
   through `hive-ttracking.clock/now-millis` so tests can pin time."
  (:require [hive-dsl.result :as r]
            [milvus-clj.api :as milvus]
            [taoensso.timbre :as log]))
;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

;; The cache lives in milvus-clj.api alongside the singleton client so a
;; reset of the client (disconnect!) invalidates implicitly. We expose the
;; same surface here for callers that prefer the resilience namespace as
;; their import seam.

(defn alive?
  "Cached liveness — true iff the singleton milvus client can reach the
   server. Cache TTL is owned by `milvus-clj.api/connected?` (~5s)."
  []
  (milvus/connected?))

(defn invalidate!
  "Force the next `alive?` to re-probe. Call after observing any failure
   that suggests the cache might be stale-positive."
  []
  (milvus/invalidate-probe-cache!))

(defn probe-once!
  "Bypass the cache and issue a fresh probe RPC. Returns true/false.
   Used by the reconnect loop's success criterion: only an actual
   round-trip counts as recovered.

   Uses `r/rescue` so any throwable becomes a clean false — the resilience
   layer wants a boolean, never an exception, when asking 'is it alive
   right now?'."
  []
  (invalidate!)
  (r/rescue false
    (let [r (alive?)]
      (when r (log/debug "milvus probe: alive"))
      r)))
