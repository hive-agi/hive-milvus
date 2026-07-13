;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.embed.port
  "Consumer-owned embedding port + injection slot.

   `IEmbedder` is the boundary hive-milvus calls for every provider
   lookup, routing decision and remote embed. The host injects a
   concrete adapter via `set-embedder!` at boot; a no-op default keeps
   the core loadable and testable standalone (no hive-mcp on the path).

   Protocol methods are `-`-prefixed and dispatch on an injected
   instance; the public fns below deref the slot so call sites stay
   free of `(current)` threading. Method contracts:

     -embed-entry             entry+collection+content => (r/ok vec) |
                              (r/err :embedder/no-provider {…}) |
                              (r/err :embedder/embed-failed {…})
     -embed-text              collection+text => vec (may throw)
     -routing-for-type[+size] memory-type[+content] =>
                              {:collection-name str :dimension int
                               :max-tokens int :provider-key kw} | nil
     -no-embed-type?          memory-type => bool
     -collection-names        => seq of chroma-style names
     -dimension-for-collection collection => int | nil
     -configured-collection-names => seq of chroma-style names
     -provider-available-for? collection => bool
     -collection-backed?      collection => bool"
  (:require [hive-dsl.result :as r]
            [malli.core :as m]))

(defonce ^:private -iembedder-defined? (atom false))

(when (compare-and-set! -iembedder-defined? false true)
  (defprotocol IEmbedder
    (-embed-entry [this entry collection-name content])
    (-embed-text [this collection-name text])
    (-routing-for-type [this memory-type])
    (-routing-for-type+size [this memory-type content])
    (-no-embed-type? [this memory-type])
    (-collection-names [this])
    (-dimension-for-collection [this collection-name])
    (-configured-collection-names [this])
    (-provider-available-for? [this collection-name])
    (-collection-backed? [this collection-name])))

(def Embedder [:fn {:error/message "must satisfy IEmbedder"} #(satisfies? IEmbedder %)])

(def ^:private default-collection "hive-mcp-memory")

(def ^:private default-routing
  {:collection-name default-collection
   :dimension       768
   :max-tokens      2048
   :provider-key    :noop})

(defrecord NoopEmbedder []
  IEmbedder
  (-embed-entry [_ _entry _collection-name _content] (r/ok []))
  (-embed-text [_ _collection-name _text] [])
  (-routing-for-type [_ _memory-type] default-routing)
  (-routing-for-type+size [_ _memory-type _content] default-routing)
  (-no-embed-type? [_ _memory-type] false)
  (-collection-names [_] [default-collection])
  (-dimension-for-collection [_ _collection-name] nil)
  (-configured-collection-names [_] [default-collection])
  (-provider-available-for? [_ _collection-name] false)
  (-collection-backed? [_ _collection-name] false))

(defonce ^:private noop-embedder (->NoopEmbedder))

(defonce ^:private slot (atom noop-embedder))

(defn set-embedder!
  "Install `embedder` (an IEmbedder) as the process-wide embedding boundary."
  [embedder]
  (when-not (satisfies? IEmbedder embedder)
    (throw (ex-info "Embedder must satisfy IEmbedder"
                    {:type :embedder/invalid-adapter
                     :adapter-class (some-> embedder class str)})))
  (reset! slot embedder))

(m/=> set-embedder! [:=> [:cat Embedder] Embedder])

(defn reset-embedder! [] (set-embedder! noop-embedder))

(m/=> reset-embedder! [:=> [:cat] Embedder])

(defn current
  "The currently-installed IEmbedder (NoopEmbedder until the host injects)."
  []
  @slot)

(m/=> current [:=> [:cat] Embedder])

(defn embed-entry [entry collection-name content]
  (-embed-entry (current) entry collection-name content))

(defn embed-text [collection-name text]
  (-embed-text (current) collection-name text))

(defn routing-for-type [memory-type]
  (-routing-for-type (current) memory-type))

(defn routing-for-type+size [memory-type content]
  (-routing-for-type+size (current) memory-type content))

(defn no-embed-type? [memory-type]
  (-no-embed-type? (current) memory-type))

(defn collection-names []
  (-collection-names (current)))

(defn dimension-for-collection [collection-name]
  (-dimension-for-collection (current) collection-name))

(defn configured-collection-names []
  (-configured-collection-names (current)))

(defn provider-available-for? [collection-name]
  (-provider-available-for? (current) collection-name))

(defn collection-backed? [collection-name]
  (-collection-backed? (current) collection-name))