(ns hive-milvus.store.rest-index
  "REST scalar-index adapter for milvus-clj 0.2.5 (whose HTTP extras are absent).
   Does not release or reload collections. Server create is asynchronous."
  (:require [clojure.data.json :as json])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
            HttpResponse$BodyHandlers]
           [java.time Duration]))

(defonce ^:private http-client
  (delay (-> (HttpClient/newBuilder)
             (.connectTimeout (Duration/ofSeconds 5))
             (.build))))

(defn post!
  "Checked Milvus v2 request. Never include the token in errors."
  [config path body]
  (let [{:keys [host port token database http-request-timeout-ms]} config
        request (doto (HttpRequest/newBuilder)
                  (.uri (URI/create (str "http://" (or host "localhost") ":" (or port 19530) path)))
                  (.timeout (Duration/ofMillis (long (or http-request-timeout-ms 30000))))
                  (.header "Content-Type" "application/json")
                  (.POST (HttpRequest$BodyPublishers/ofString
                          (json/write-str (cond-> body database (assoc :dbName database))))))
        _ (when token (.header request "Authorization" (str "Bearer " token)))
        response (.send @http-client (.build request)
                        (HttpResponse$BodyHandlers/ofString))
        status (.statusCode response)]
    (when-not (<= 200 status 299)
      (throw (ex-info "Milvus index HTTP failure" {:path path :status status})))
    (let [result (json/read-str (.body response) :key-fn keyword)]
      (when-not (= 0 (:code result))
        (throw (ex-info "Milvus index request refused"
                        {:path path :code (:code result) :message (:message result)})))
      (:data result))))

(defn ensure-fields!
  "Preserve existing field indexes; submit missing INVERTED definitions.
   Return true only when every requested field reports Finished. Pending
   builds return false so callers re-probe, failures throw. No release/load."
  [request! collection fields]
  (let [base {:collectionName collection}
        names (request! "/v2/vectordb/indexes/list" base)
        indexes (mapcat #(request! "/v2/vectordb/indexes/describe"
                                   (assoc base :indexName %)) names)
        wanted (filter #(contains? (set fields) (:fieldName %)) indexes)
        present (set (map :fieldName wanted))]
    (when (some #(= "Failed" (:indexState %)) wanted)
      (throw (ex-info "Milvus reports a failed scalar index build"
                      {:indexes (vec wanted)})))
    (doseq [field fields :when (not (contains? present field))]
      (request! "/v2/vectordb/indexes/create"
                (assoc base :indexParams [{:fieldName field
                                          :indexName (str field "_idx")
                                          :indexType "INVERTED"}])))
    (and (= (set fields) present)
         (every? #(= "Finished" (:indexState %)) wanted))))
