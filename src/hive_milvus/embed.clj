(ns hive-milvus.embed
  "Ollama embedding client."
  (:require [clojure.data.json :as json]
            [hive-dsl.result :as r])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers
                          HttpResponse$BodyHandlers]))

(def ^:private http-client
  (delay (-> (HttpClient/newBuilder)
             (.connectTimeout (java.time.Duration/ofSeconds 30))
             (.build))))

(defn embed-text
  "Get embedding vector from Ollama. Returns Result."
  [text ollama-host model]
  (r/try-effect* :embed/failed
    (let [body    (json/write-str {:model model :prompt (subs text 0 (min 8000 (count text)))})
          request (-> (HttpRequest/newBuilder)
                      (.uri (URI/create (str "http://" ollama-host "/api/embeddings")))
                      (.header "Content-Type" "application/json")
                      (.POST (HttpRequest$BodyPublishers/ofString body))
                      (.timeout (java.time.Duration/ofSeconds 60))
                      (.build))
          resp    (.send @http-client request (HttpResponse$BodyHandlers/ofString))
          parsed  (json/read-str (.body resp) :key-fn keyword)]
      (:embedding parsed))))
