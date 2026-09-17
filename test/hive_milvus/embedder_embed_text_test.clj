;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.embedder-embed-text-test
  "The :embed-text hand-off: a caller whose :content is ciphertext supplies the
   text to embed beside it, and the store embeds that text and never writes it."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]
            [hive-milvus.embedder :as embedder]
            [hive-milvus.store.entries :as entries]
            [hive-milvus.store.schema :as schema]))

(defrecord CapturingEmbedder [seen]
  port/IEmbedder
  (-embed-entry [_ _entry _collection-name content]
    (swap! seen conj content)
    (r/ok [0.5 0.5]))
  (-embed-text [_ _collection-name _text] [0.5 0.5])
  (-routing-for-type [_ _memory-type] {:collection "c" :dimension 2})
  (-routing-for-type+size [_ _memory-type _content] {:collection "c" :dimension 2})
  (-no-embed-type? [_ _memory-type] false)
  (-collection-names [_] ["c"])
  (-dimension-for-collection [_ _collection-name] 2)
  (-configured-collection-names [_] ["c"])
  (-provider-available-for? [_ _collection-name] true)
  (-collection-backed? [_ _collection-name] true))

(use-fixtures :each
  (fn [t]
    (port/reset-embedder!)
    (try (t) (finally (port/reset-embedder!)))))

(deftest embed-text-is-what-gets-embedded
  (let [seen (atom [])]
    (port/set-embedder! (->CapturingEmbedder seen))
    (testing "present: embedded instead of the ciphertext content"
      (is (r/ok? (embedder/embed-for-entry {:type "note" :content "#hive/work 1\nkid: x\nQUJD"
                                            :embed-text "the plaintext"}
                                           "c")))
      (is (= "the plaintext" (last @seen))))
    (testing "absent: content is embedded as before"
      (embedder/embed-for-entry {:type "note" :content "plain content"} "c")
      (is (= "plain content" (last @seen))))))

(deftest embed-text-never-reaches-the-record
  (let [record (schema/entry->record-pure {:id "e" :type "note" :content "ciphertext"
                                           :embed-text "the plaintext"}
                                          "c" [0.5 0.5])]
    (is (not (contains? record :embed-text)))
    (is (not-any? #(= "the plaintext" %) (vals record)))))

(deftest the-store-declares-the-capability
  (is (some #{:embed-text} entries/capabilities)))
