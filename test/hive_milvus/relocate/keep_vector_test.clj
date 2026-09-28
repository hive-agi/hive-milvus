(ns hive-milvus.relocate.keep-vector-test
  "An in-place update whose text to embed is unchanged keeps the stored
   vector instead of calling the embedder. Measured need (2026-09-28): the
   seal migration rewrites every row's :content to ciphertext and hands the
   same plaintext back as :embed-text; re-embedding each row through Ollama
   held the pass at ~0.2 rows/s.

   Seams are ports, not redefinitions: the embedder is a recording
   IEmbedder installed through `port/set-embedder!`, the vector read is the
   `read` argument of `boundary/keep-vector`."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]
            [hive-milvus.relocate.boundary :as boundary]
            [hive-milvus.relocate.promoters.record :as p-record]))

;; ============================================================
;; A recording embedder: counts calls, answers a fixed-width vector
;; ============================================================

(defrecord RecordingEmbedder [calls dim]
  port/IEmbedder
  (-embed-entry [_ _entry collection-name content]
    (swap! calls conj [collection-name content])
    (r/ok (vec (repeat dim 0.5))))
  (-embed-text [_ _collection-name _text] (vec (repeat dim 0.5)))
  (-routing-for-type [_ _memory-type] nil)
  (-routing-for-type+size [_ _memory-type _content] nil)
  (-no-embed-type? [_ _memory-type] false)
  (-collection-names [_] [])
  (-dimension-for-collection [_ _collection-name] dim)
  (-configured-collection-names [_] [])
  (-provider-available-for? [_ _collection-name] true)
  (-collection-backed? [_ _collection-name] true))

(def ^:private coll "hive_mcp_memory_4d")

(def ^:private calls (atom []))

(use-fixtures :each
  (fn [t]
    (reset! calls [])
    (port/set-embedder! (->RecordingEmbedder calls 4))
    (try (t) (finally (port/reset-embedder!)))))

(defn- sealing-rewrite
  "The bundle a seal rewrite produces: plaintext stored, ciphertext written,
   the same plaintext handed over to embed."
  [plaintext]
  (let [existing {:id "e1" :type "note" :content plaintext :tags ["a"]}]
    {:id          "e1"
     :src-coll    coll
     :target-coll coll
     :existing    existing
     :entry       (assoc existing :content "HIVE-SEAL-WORK:ciphertext" :embed-text plaintext)}))

;; ============================================================
;; vector-still-valid? — pure
;; ============================================================

(deftest a-seal-rewrite-keeps-the-text-to-embed
  (is (true? (p-record/vector-still-valid? (sealing-rewrite "quarterly numbers")))))

(deftest changed-text-needs-a-new-vector
  (is (false? (p-record/vector-still-valid?
                (assoc-in (sealing-rewrite "quarterly numbers") [:entry :embed-text] "other")))))

(deftest a-move-to-another-collection-needs-a-new-vector
  (is (false? (p-record/vector-still-valid?
                (assoc (sealing-rewrite "quarterly numbers") :target-coll "hive_mcp_memory_8d")))))

(deftest a-row-stored-as-ciphertext-is-re-embedded
  (testing "the stored vector came from text the backend cannot see, so it is not trusted"
    (is (false? (p-record/vector-still-valid?
                  (assoc-in (sealing-rewrite "quarterly numbers")
                            [:existing :content] "HIVE-SEAL-WORK:older"))))))

(deftest no-existing-row-no-kept-vector
  (is (false? (p-record/vector-still-valid? (dissoc (sealing-rewrite "x") :existing)))))

(deftest equal-text-is-exactly-what-keeps-the-vector
  (let [res (tc/quick-check
              200
              (prop/for-all [stored gen/string-alphanumeric
                             handed gen/string-alphanumeric]
                (= (= stored handed)
                   (p-record/vector-still-valid?
                     (assoc-in (sealing-rewrite stored) [:entry :embed-text] handed)))))]
    (is (:pass? res) (pr-str (:shrunk res)))))

;; ============================================================
;; keep-vector — the read is injected
;; ============================================================

(defn- reads-ok [v] (fn [_coll _id] (r/ok v)))

(deftest the-stored-vector-is-kept-when-valid
  (is (= [1.0 2.0 3.0 4.0]
         (:kept-embedding (boundary/keep-vector (reads-ok [1.0 2.0 3.0 4.0])
                                                (sealing-rewrite "q"))))))

(deftest nothing-is-read-when-the-vector-cannot-be-kept
  (let [read-calls (atom 0)
        read       (fn [_ _] (swap! read-calls inc) (r/ok [1.0 2.0 3.0 4.0]))
        bundle     (assoc-in (sealing-rewrite "q") [:entry :embed-text] "changed")]
    (is (= bundle (boundary/keep-vector read bundle)))
    (is (zero? @read-calls))))

(deftest a-failed-or-odd-read-falls-back-to-embedding
  (let [bundle (sealing-rewrite "q")]
    (testing "read failed"
      (is (nil? (:kept-embedding (boundary/keep-vector (fn [_ _] (r/err :boom {})) bundle)))))
    (testing "no vector stored"
      (is (nil? (:kept-embedding (boundary/keep-vector (reads-ok nil) bundle)))))
    (testing "a vector of the wrong width for the collection"
      (is (nil? (:kept-embedding (boundary/keep-vector (reads-ok [1.0 2.0]) bundle)))))))

;; ============================================================
;; build-target-record — the embedder is skipped only with a kept vector
;; ============================================================

(deftest a-kept-vector-builds-the-record-without-the-embedder
  (let [res (p-record/build-target-record
              (assoc (sealing-rewrite "q") :kept-embedding [1.0 2.0 3.0 4.0]))]
    (is (r/ok? res))
    (is (= [1.0 2.0 3.0 4.0] (:embedding (:ok res))))
    (is (empty? @calls) "the embedder must not be called")))

(deftest without-a-kept-vector-the-entry-is-embedded
  (let [res (p-record/build-target-record (sealing-rewrite "q"))]
    (is (r/ok? res))
    (is (= [[coll "q"]] @calls) "embedded once, on the handed-over plaintext")))
