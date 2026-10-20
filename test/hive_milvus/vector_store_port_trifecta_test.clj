(ns hive-milvus.vector-store-port-trifecta-test
  "Schema, property and conformance checks for how the Milvus adapter extends
   the host's IVectorCollectionStore. The defect: install! extended with
   :get-collection etc. while the port's methods are -get-collection etc., so
   every call failed with 'No implementation of method' and the host reported
   it as reload poisoning."
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.generators :as gen]
            [hive-schemas.test :as hst]
            [hive-test.properties :refer [defprop-metamorphic]]
            [hive-milvus.vector-store :as vs]))

(defprotocol StandInPort
  "The host port's method names, as hive-mcp.protocols.vector declares them."
  (-configure [this opts])
  (-get-collection [this coll-name])
  (-create-collection [this coll-name opts])
  (-delete-collection [this coll])
  (-add [this coll records opts])
  (-get [this coll opts])
  (-query [this coll embedding opts])
  (-delete [this coll opts])
  (-update [this coll records]))

(def ^:private port-keys
  [:-configure :-get-collection :-create-collection :-delete-collection
   :-add :-get :-query :-delete :-update])

(def ^:private key-cases
  [port-keys
   [:configure :get-collection :add]
   [:-add :-nope]
   []])

(def ^:private KeyCase (into [:enum] key-cases))

(def ^:private known
  #{"configure" "get-collection" "create-collection" "delete-collection"
    "add" "get" "query" "delete" "update"})

(defn- bare [k] (let [n (name k)] (if (= \- (first n)) (subs n 1) n)))

(defn- impl-law
  "Exactly the keys that name a known method, each mapped to a fn."
  [ks out]
  (and (= (set (filter (comp known bare) ks)) (set (keys out)))
       (every? fn? (vals out))))

(hst/deftrifecta-from-schema impl-map-contract
  #'vs/impl-map
  {:in KeyCase
   :out [:map-of :keyword fn?]
   :rel impl-law
   :contract true
   :num-tests 30
   :seed 0
   :n-cases 4})

(defn- dashed [ks] (mapv #(keyword (str "-" (bare %))) ks))

(defn- impl-names [ks] (set (map bare (keys (vs/impl-map ks)))))

(defprop-metamorphic the-dash-spelling-names-the-same-methods
  impl-names
  dashed
  =
  (gen/fmap vec (gen/set (gen/elements (vec (map keyword known)))))
  {:num-tests 60})

(deftest the-adapter-implements-every-method-of-the-port
  (#'vs/install! StandInPort)
  (let [store (vs/->MilvusVectorCollectionStore)]
    (is (satisfies? StandInPort store))
    (testing "a real dispatch through the port reaches the adapter"
      (is (identical? store (-configure store {}))))))

(deftest a-port-method-without-an-impl-is-refused-loudly
  (let [wider (update StandInPort :sigs assoc :-reindex {:name '-reindex})]
    (is (= {:missing [:-reindex]}
           (try (#'vs/install! wider) nil
                (catch clojure.lang.ExceptionInfo e (ex-data e)))))))
