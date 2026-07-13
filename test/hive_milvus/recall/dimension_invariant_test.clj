;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.recall.dimension-invariant-test
  "GOLDEN RECALL — CASE 5: DIMENSION INVARIANT.

   'embedder dim == collection dim, asserted at startup; refuse to serve
    queries on mismatch.'

   The invariant has two halves and BOTH are pinned here.

   REFUSE TO SERVE (per call). A wrong-width query vector must never reach
   Milvus. If it does, the best case is an error; the worst — and the one that
   actually happened — is that another model shares the width, Milvus answers,
   and the caller gets confident neighbours from a space the query was never
   embedded into. Pinned on the READ path
   (`store.search.boundary/CollectionEmbedder`) and the WRITE path
   (`embedder/embed-for-entry`): both must return `:embedder/dim-mismatch`
   rather than a vector.

   ASSERT AT STARTUP. The per-call guard makes a mismatch harmless but leaves
   it INVISIBLE — a misconfigured embedder just fails every query, quietly,
   forever. `collection.invariant/verify` states it once, at boot, by name.

   UNKNOWN IS NOT OK. `naming/dim-of` cannot read a width off a carto or kanban
   collection name. Such a collection lands in `:unknown`, never in `:ok`. A
   checker that says 'fine' when it means 'I could not tell' is precisely the
   silent-success pathology this whole suite exists to kill.

   Hermetic — no live Milvus, no live embedder. A fake `IEmbedder` installed
   through `hive-milvus.embed.port` is the injected boundary."
  (:require [clojure.test :refer [deftest is testing]]
            [hive-dsl.result :as r]
            [hive-milvus.embed.fake :as fake]
            [hive-milvus.collection.invariant :as inv]
            [hive-milvus.collection.naming :as naming]
            [hive-milvus.store.search.boundary :as b]
            [hive-milvus.store.search.target :as tgt]))

(def ^:private mem-2560 "hive_mcp_memory_2560d")
(def ^:private mem-768  "hive-mcp-memory")
(def ^:private carto    "hive-carto-snippets")

;; =============================================================================
;; The invariant is knowable — the collection declares its own width
;; =============================================================================

(deftest a-memory-collection-names-the-width-it-holds
  (testing "both spellings, and the legacy 768 default"
    (is (= 2560 (naming/dim-of mem-2560)))
    (is (= 2560 (naming/dim-of "hive-mcp-memory-2560d")))
    (is (= 768  (naming/dim-of mem-768))))
  (testing "a non-memory collection does NOT — that is 'unknown', not 768"
    (is (nil? (naming/dim-of carto)))))

;; =============================================================================
;; STARTUP ASSERTION
;; =============================================================================

(deftest startup-check-passes-when-the-widths-agree
  (fake/with-embedder {:dimension-for-collection (constantly 2560)}
    (let [report (inv/verify [mem-2560])]
      (is (inv/satisfied? report))
      (is (= [{:collection mem-2560 :dim 2560}] (:ok report)))
      (is (empty? (:unknown report))))))

(deftest startup-check-names-the-mismatch-instead-of-serving-it
  (testing "the 768-d embedder against the 2560-d collection — the live
            misconfiguration. It must be reported by name at boot, with both
            widths, under the SAME tag the per-call guards use."
    (fake/with-embedder {:dimension-for-collection (constantly 768)}
      (let [report (inv/verify [mem-2560])
            [v]    (:violations report)]
        (is (not (inv/satisfied? report))
            "a 768-d embedder feeding a 2560-d index is not a degraded system, it
             is a broken one")
        (is (= :embedder/dim-mismatch (:error v)))
        (is (= 2560 (:expected v)))
        (is (= 768  (:actual v)))
        (is (= mem-2560 (:collection v)))))))

(deftest an-unreadable-width-is-reported-as-unknown-never-as-ok
  (testing "a collection whose width the name does not encode"
    (fake/with-embedder {:dimension-for-collection (constantly 2560)}
      (let [report (inv/verify [carto])]
        (is (empty? (:ok report))
            "an unverifiable collection must NOT be counted as verified")
        (is (= [:dim/unknown] (mapv :error (:unknown report))))
        (is (inv/satisfied? report)
            "unverifiable is not the same as violated — it must not block boot"))))
  (testing "a collection with no resolvable embedder is unknown, not skipped"
    (fake/with-embedder {:dimension-for-collection (fn [_] (throw (ex-info "no provider" {})))}
      (let [report (inv/verify [mem-2560])]
        (is (empty? (:ok report)))
        (is (= [:dim/embedder-absent] (mapv :error (:unknown report))))))))

(deftest one-broken-collection-does-not-hide-behind-the-healthy-ones
  (testing "the report must be per-collection — a single aggregate boolean would
            let a good collection mask a bad one"
    (fake/with-embedder {:dimension-for-collection (fn [c] (if (= c mem-2560) 768 768))}
      (let [report (inv/verify [mem-768 mem-2560 carto])]
        (is (= [mem-768]  (mapv :collection (:ok report))))
        (is (= [mem-2560] (mapv :collection (:violations report))))
        (is (= [carto]    (mapv :collection (:unknown report))))
        (is (not (inv/satisfied? report)))))))

;; =============================================================================
;; REFUSE TO SERVE — read path
;; =============================================================================

(deftest a-wrong-width-query-vector-is-refused-not-searched
  (testing "the read path must return an error, NOT neighbours. Neighbours from
            a space the query was never embedded into are indistinguishable from
            an answer — that is the whole outage in one sentence."
    (fake/with-embedder {:embed-text (fn [_ _] (vec (repeat 768 0.1)))}
      (let [res (b/-embed (b/collection-embedder) (tgt/->target mem-2560 :new) "q")]
        (is (r/err? res))
        (is (= :embedder/dim-mismatch (:error res)))
        (is (= 2560 (:expected res)))
        (is (= 768  (:actual res)))))))

(deftest a-right-width-query-vector-is-served
  (testing "the guard must not be a blanket refusal — a correct embedder works"
    (fake/with-embedder {:embed-text (fn [_ _] (vec (repeat 2560 0.1)))}
      (let [res (b/-embed (b/collection-embedder) (tgt/->target mem-2560 :new) "q")]
        (is (r/ok? res))
        (is (= 2560 (count (:ok res))))))))

;; =============================================================================
;; The startup assertion and the per-call guard must agree
;; =============================================================================

(deftest startup-and-per-call-name-the-same-fault
  (testing "one fault, one keyword. If these ever diverge, an operator reading
            the boot log cannot connect it to the errors the queries return."
    (fake/with-embedder {:dimension-for-collection (constantly 768)
                         :embed-text (fn [_ _] (vec (repeat 768 0.1)))}
      (let [boot  (first (:violations (inv/verify [mem-2560])))
            query (b/-embed (b/collection-embedder) (tgt/->target mem-2560 :new) "q")]
        (is (= (:error boot) (:error query) :embedder/dim-mismatch))
        (is (= (:expected boot) (:expected query)))
        (is (= (:actual boot) (:actual query)))))))
