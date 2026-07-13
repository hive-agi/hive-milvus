;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: MIT

(ns hive-milvus.embed.fake
  "Test-only configurable IEmbedder (provider-behaviour-as-data). Pass a map
   of method-key -> fn to `embedder`; unspecified methods fall back to a
   no-op default. `with-embedder` installs one for the body then restores."
  (:require [hive-dsl.result :as r]
            [hive-milvus.embed.port :as port]))

(defn embedder
  [overrides]
  (let [f (fn [k d] (get overrides k d))]
    (reify port/IEmbedder
      (-embed-entry [_ e c ct] ((f :embed-entry (fn [& _] (r/ok []))) e c ct))
      (-embed-text [_ c t] ((f :embed-text (fn [& _] [])) c t))
      (-routing-for-type [_ t] ((f :routing-for-type (constantly nil)) t))
      (-routing-for-type+size [_ t c] ((f :routing-for-type+size (constantly nil)) t c))
      (-no-embed-type? [_ t] ((f :no-embed-type? (constantly false)) t))
      (-collection-names [_] ((f :collection-names (constantly []))))
      (-dimension-for-collection [_ c] ((f :dimension-for-collection (constantly nil)) c))
      (-configured-collection-names [_] ((f :configured-collection-names (constantly []))))
      (-provider-available-for? [_ c] ((f :provider-available-for? (constantly false)) c))
      (-collection-backed? [_ c] ((f :collection-backed? (constantly false)) c)))))

(defmacro with-embedder
  [overrides & body]
  `(let [prev# (port/current)]
     (port/set-embedder! (embedder ~overrides))
     (try ~@body (finally (port/set-embedder! prev#)))))
