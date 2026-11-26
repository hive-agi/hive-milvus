(ns hive-milvus.store.space
  "Embedding-space provenance of a Milvus collection.

   A dimension match says only that two vectors have the same width. A
   collection also keeps the full space it was created in: provider, model,
   exact digest, dimension, metric, normalization, and the query-template and
   document-serializer revisions. That space lives in the collection's own
   properties (string -> string) and is checked whenever an existing
   collection is opened, so vectors of two spaces never share a collection.

   The value layer (stamp, admit) is hive-spi.embeddings.space. This
   namespace converts its flat scalar stamp to and from Milvus properties,
   and reads and writes those properties over the Milvus v2 REST API. The
   REST API is served on the same port as gRPC, so this works for both
   transports. milvus-clj 0.3.1 neither sends nor returns collection
   properties.

     stamp->properties     stamp as a Milvus string property map
     properties->stamp     Milvus properties (map or [{key value}]) as a stamp
     read-stamp            IO: the stamp of an existing collection
     write-stamp!          IO: stamp an existing collection with a space
     admit!                guard: stamped ok, legacy warns, other space throws"
  (:require [hive-milvus.store.rest-index :as rest-index]
            [hive-spi.embeddings.space :as space]
            [milvus-clj.config :as milvus-config]
            [taoensso.timbre :as log]))

;; SPDX-License-Identifier: MIT

(def metric
  "Metric of every collection hive-milvus creates: milvus-clj's
   default-memory-index scores the embedding field with COSINE."
  :cosine)

(defn space-for-collection
  "SPACE with the metric hive-milvus collections score with, or nil."
  [space]
  (when space (space/->space (assoc space :metric metric))))

(defn stamp->properties
  "The Milvus collection properties that stamp a collection with SPACE:
   every hive-spi stamp key by name, every value as a string."
  [space]
  (into {}
        (map (fn [[k v]] [(name k) (str v)]))
        (space/space->metadata space)))

(defn- props->map
  "Milvus answers properties as [{:key k :value v}] (REST describe) or as a
   map; either way, a string-keyed map."
  [props]
  (cond
    (map? props)        (into {} (map (fn [[k v]] [(name k) v])) props)
    (sequential? props) (into {} (keep (fn [p] (when-let [k (or (:key p) (get p "key"))]
                                                 [(name k) (or (:value p) (get p "value"))])))
                              props)
    :else               {}))

(defn properties->stamp
  "The keyword-keyed stamp held in Milvus PROPS: only the :embed-* keys, the
   dimension read back as a long. Absent or empty PROPS answer {}."
  [props]
  (into {}
        (keep (fn [[k v]]
                (when (and (some? v) (.startsWith ^String k "embed-"))
                  [(keyword k)
                   (if (and (= k "embed-dimension") (string? v)) (parse-long v) v)])))
        (props->map props)))

(defn request!
  "Boundary: one checked Milvus v2 REST call (PATH, BODY) under the
   configured connection. Tests redefine it."
  [path body]
  (rest-index/post! (milvus-config/get-config) path body))

(defn read-stamp
  "Boundary: IO. The stamp of the existing collection COLLECTION-NAME ({}
   when it carries none). Throws what the request throws."
  [collection-name]
  (properties->stamp
   (:properties (request! "/v2/vectordb/collections/describe"
                          {:collectionName collection-name}))))

(defn write-stamp!
  "Boundary: IO. Stamp COLLECTION-NAME with SPACE in its properties."
  [collection-name space]
  (request! "/v2/vectordb/collections/alter_properties"
            {:collectionName collection-name
             :properties     (stamp->properties space)})
  nil)

(defonce ^:private legacy-warned (atom #{}))

(defn reset-legacy-warnings!
  "Test helper: forget which legacy collections were already warned about."
  []
  (reset! legacy-warned #{}))

(defn admit!
  "Guard collection COLLECTION-NAME, whose stamp is MD, for vectors of SPACE.

     stamped, same space  => the hive-spi Admission, :verdict :stamped
     no stamp             => :verdict :legacy, warned once per collection
     stamped, other space => throws ex-info :embedding-space-mismatch naming
                             both spaces, the differing keys, the collection
                             and the remedy #{:isolate :re-embed}"
  [collection-name space md]
  (let [{:keys [verdict space-id] :as admission} (space/admit collection-name space md)]
    (when (and (= :legacy verdict) (not (contains? @legacy-warned collection-name)))
      (swap! legacy-warned conj collection-name)
      (log/warn "hive-milvus: embedding space unrecorded for legacy collection"
                collection-name "- admitting vectors of space" space-id
                "unverified; isolate or re-embed to stamp it"))
    admission))

(defn guard-existing!
  "Admit the existing COLLECTION-NAME for vectors of SPACE: read its stamp
   and `admit!` it. A stamp that cannot be read is logged and the collection
   admitted unverified (nil); a mismatch throws."
  [collection-name space]
  (let [md (try (read-stamp collection-name)
                (catch Throwable t
                  (log/warn "hive-milvus: could not read the embedding space of"
                            collection-name "- admitting it unverified:" (ex-message t))
                  ::unread))]
    (when-not (= ::unread md)
      (admit! collection-name space md))))

(defn stamp-new!
  "Stamp the just-created COLLECTION-NAME with SPACE. A failed stamp is
   logged, not thrown: the collection exists and holds no vectors yet, and
   it reads back as legacy until a later open stamps nothing over it."
  [collection-name space]
  (try (write-stamp! collection-name space)
       (catch Throwable t
         (log/warn "hive-milvus: could not stamp the embedding space of new collection"
                   collection-name "-" (ex-message t)))))
