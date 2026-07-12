;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.collection.invariant
  "THE DIMENSION INVARIANT: the width of the vector the embedder produces for a
   collection MUST equal the width that collection holds.

   When it does not, nothing crashes. Milvus rejects an insert with error 1804,
   which a write path can swallow; and a SEARCH with a wrong-width query vector
   either errors or — the dangerous case, when another model happens to share
   the width — returns confident neighbours from a space the query was never
   embedded into. That is noise wearing the costume of an answer, and it is the
   shape the 2026-07-12 outage wore.

   `hive-milvus.store.search.boundary/CollectionEmbedder` and
   `hive-milvus.embedder/embed-for-entry` already REFUSE to serve on a mismatch
   (`:embedder/dim-mismatch`) — read path and write path. That is the per-call
   half of the invariant, and it is the half that keeps a wrong vector out of
   the index.

   This namespace is the OTHER half: assert it ONCE, at startup, so a
   misconfiguration is discovered when the server boots rather than on the
   first query of the day, silently, per call, forever.

   Pure core / effectful shell:
     `check`      — pure, one collection.
     `violations` — pure, a set of already-read (collection, expected, actual).
     `verify`     — the boundary: reads live embedder dims, calls the pure core.

   UNKNOWN IS NOT OK. `naming/dim-of` returns nil for a collection whose width
   is not knowable from its name (anything that is not a memory collection).
   Those are reported under `:unknown` — never folded into `:ok`. A checker
   that reports 'fine' when it means 'I could not tell' is the exact failure
   this file exists to prevent."
  (:require [hive-dsl.result :as r]
            [hive-mcp.embeddings.service :as embed-svc]
            [hive-milvus.collection.naming :as naming]
            [taoensso.timbre :as log]))

;; =============================================================================
;; Pure core
;; =============================================================================

(defn check
  "Pure. The invariant for ONE collection.

     expected — the width the collection HOLDS (from its name, via naming/dim-of)
     actual   — the width the embedder PRODUCES

   => r/ok  {:collection … :dim n}                       aligned
      r/err :dim/unknown        {:collection …}          expected is unknowable
      r/err :dim/embedder-absent{:collection …}          actual is unreadable
      r/err :embedder/dim-mismatch {:collection … :expected … :actual …}

   The mismatch tag is deliberately the SAME keyword the per-call guards emit,
   so a startup report and a query refusal name the same fault."
  [{:keys [collection expected actual]}]
  (cond
    (nil? expected)
    (r/err :dim/unknown
           {:collection collection
            :message    (str "cannot determine the width " collection " holds from "
                             "its name — the invariant is unverifiable, not satisfied")})

    (nil? actual)
    (r/err :dim/embedder-absent
           {:collection collection
            :expected   expected
            :message    (str "no embedder resolves for " collection " — queries "
                             "against it cannot be served")})

    (not= expected actual)
    (r/err :embedder/dim-mismatch
           {:collection collection
            :expected   expected
            :actual     actual
            :message    (str collection " holds " expected "-d vectors but its "
                             "embedder produces " actual "-d. Queries would search "
                             "a space they were never embedded into.")})

    :else
    (r/ok {:collection collection :dim expected})))

(defn violations
  "Pure. Partition already-read `readings` — a seq of
   `{:collection … :expected … :actual …}` — into the three buckets.

   => {:ok [{:collection :dim}…]
       :violations [err…]   the invariant is BROKEN. Refuse to serve.
       :unknown    [err…]   the invariant is UNVERIFIABLE. Say so, loudly.}"
  [readings]
  (let [checked (mapv (fn [x] [x (check x)]) readings)]
    {:ok         (vec (for [[_ res] checked :when (r/ok? res)] (:ok res)))
     :violations (vec (for [[_ res] checked
                            :when (and (not (r/ok? res))
                                       (= :embedder/dim-mismatch (:error res)))]
                        res))
     :unknown    (vec (for [[_ res] checked
                            :when (and (not (r/ok? res))
                                       (not= :embedder/dim-mismatch (:error res)))]
                        res))}))

(defn satisfied?
  "Pure. True only when NOTHING is broken. `:unknown` does not break the
   invariant (a non-memory collection has no name-derivable width) but it never
   satisfies it either — it is simply out of scope for this check."
  [report]
  (empty? (:violations report)))

;; =============================================================================
;; Boundary — read the live embedder widths
;; =============================================================================

(defn read-dims
  "BOUNDARY. For each collection name, pair the width it HOLDS (from its name)
   with the width its embedder PRODUCES (from the live provider registry).

   An embedder that cannot be resolved reads as nil `:actual` — reported as
   `:dim/embedder-absent`, never skipped."
  [collection-names]
  (mapv (fn [coll]
          {:collection coll
           :expected   (naming/dim-of coll)
           :actual     (try (embed-svc/get-dimension-for coll)
                            (catch Throwable _ nil))})
        collection-names))

(defn verify
  "BOUNDARY. The startup assertion. Reads the live widths and returns the
   `violations` report. Never throws.

   0-arity checks every collection the embedding service has been configured
   with."
  ([] (verify (keys (embed-svc/list-configured-collections))))
  ([collection-names] (violations (read-dims collection-names))))

(defn verify!
  "BOUNDARY. `verify`, but LOUD. Logs one ERROR per violation and one WARN per
   unverifiable collection, then returns the report so the caller can decide
   whether to refuse to start.

   Call this from addon init. It does not throw: a violation must be visible
   in the log of a server that is still up (so an operator can read it) rather
   than turn a misconfigured embedder into a boot loop."
  ([] (verify! (keys (embed-svc/list-configured-collections))))
  ([collection-names]
   (let [{:keys [ok violations unknown] :as report} (verify collection-names)]
     (doseq [v violations]
       (log/error "DIMENSION INVARIANT VIOLATED —" (:message v)
                  {:collection (:collection v)
                   :expected   (:expected v)
                   :actual     (:actual v)}))
     (doseq [u unknown]
       (log/warn "dimension invariant unverifiable:" (:message u)))
     (when (and (seq ok) (empty? violations))
       (log/info "dimension invariant holds for" (count ok) "collection(s)"))
     report)))
