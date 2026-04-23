(ns hive-milvus.config-test
  "Tests for hive-milvus.config — MilvusConfig defconfig.

   We replicate three of the four hive-di.testing/defconfig-tests
   generators inline (totality, defaults-only, field-mutations) so we
   can substitute a non-empty keyword generator for the :transport
   field. The default `gen-value-for-type :keyword` in hive-di v0.2.0
   can emit `(keyword \"\")` = `:`, which pr-str → \":\" → fails
   `clojure.edn/read-string` (\"Invalid token: :\"). That collapses
   the auto-generated roundtrip property for any defconfig with a
   :keyword field. See the corresponding pain-point memory.

   The handful of explicit deftests below cover Milvus-specific
   semantics (transport coercion, optional :token, env override paths)."
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest testing is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [hive-di.resolve :as resolve]
            [hive-di.testing :as ditest]
            [hive-dsl.result :as r]
            [hive-milvus.config :as config]))

;; =============================================================================
;; Generator replacements — works around upstream `:` keyword bug
;; =============================================================================

(def ^:private gen-nonempty-keyword
  (gen/fmap keyword (gen/such-that seq gen/string-alphanumeric)))

(defn- gen-value-for-type [type-kw]
  (if (= :keyword type-kw)
    gen-nonempty-keyword
    (ditest/gen-value-for-type type-kw)))

(defn- gen-string-for-type [type-kw]
  (if (= :keyword type-kw)
    (gen/fmap name gen-nonempty-keyword)
    (ditest/gen-string-for-type type-kw)))

(defn- gen-config-overrides [fields]
  (let [field-gens (mapv (fn [[field-kw {:keys [type]}]]
                           (gen/one-of
                             [(gen/return nil)
                              (gen/return [field-kw nil])
                              (gen/return [field-kw ""])
                              (gen/fmap #(vector field-kw %) (gen-value-for-type type))
                              (gen/fmap #(vector field-kw %) (gen-string-for-type type))]))
                         fields)]
    (gen/fmap (fn [entries] (into {} (remove nil? entries)))
              (apply gen/tuple field-gens))))

(defn- gen-mock-env [fields]
  (let [env-fields (filterv (fn [[_ spec]] (= :source/env (:source spec))) fields)
        env-gens   (mapv (fn [[_ {:keys [env-var type]}]]
                           (gen/one-of
                             [(gen/return nil)
                              (gen/return [env-var ""])
                              (gen/fmap #(vector env-var %)
                                        (gen-string-for-type type))]))
                         env-fields)]
    (gen/fmap (fn [entries]
                (let [env-map (into {} (remove nil? entries))]
                  {:env-map env-map :env-fn #(get env-map %)}))
              (apply gen/tuple env-gens))))

;; =============================================================================
;; Generated property tests (replacement for defconfig-tests)
;; =============================================================================

(defspec MilvusConfig-totality 50
  (prop/for-all [overrides (gen-config-overrides config/MilvusConfig-fields)
                 mock      (gen-mock-env config/MilvusConfig-fields)]
    (let [result (try
                   (resolve/resolve-config config/MilvusConfig-fields
                                           overrides
                                           {:env-fn (:env-fn mock)})
                   (catch Throwable t {:threw t}))]
      (or (r/ok? result) (r/err? result)))))

(defspec MilvusConfig-roundtrip 50
  (prop/for-all [overrides (gen-config-overrides config/MilvusConfig-fields)]
    (let [result (resolve/resolve-config config/MilvusConfig-fields
                                         overrides
                                         {:env-fn (constantly nil)})]
      (if (r/ok? result)
        (let [resolved (:ok result)]
          (= resolved (edn/read-string (pr-str resolved))))
        true))))

(deftest MilvusConfig-defaults-only
  (let [result (ditest/resolve-with-defaults-only config/MilvusConfig-fields)]
    (is (r/ok? result)
        (str "Defaults-only resolution should succeed, got: " (pr-str result)))))

(deftest MilvusConfig-field-mutations
  (doseq [[field-kw _spec] config/MilvusConfig-fields]
    (let [result (try
                   (ditest/resolve-with-field-removed config/MilvusConfig-fields field-kw)
                   (catch Throwable t {:threw t}))]
      (is (or (r/ok? result) (r/err? result))
          (str "Removing default for " field-kw
               " should return a Result, not throw")))))

;; =============================================================================
;; Targeted smoke tests — Milvus-specific semantics
;; =============================================================================

(defn- mock-env [m]
  {:env-fn #(get m %)})

(deftest defaults-only-shape
  (testing "no env, no overrides → all typed defaults present"
    (let [r (config/resolve-MilvusConfig {} (mock-env {}))]
      (is (r/ok? r))
      (let [v (:ok r)]
        (is (= "localhost"        (:host v)))
        (is (= 19530              (:port v)))
        (is (= :grpc              (:transport v)))
        (is (nil?                 (:token v)))
        (is (= "default"          (:database v)))
        (is (= false              (:secure v)))
        (is (= "hive_mcp_memory"  (:collection-name v)))))))

(deftest env-override-paths
  (testing "MILVUS_* env vars feed each field with proper coercion"
    (let [r (config/resolve-MilvusConfig
              {}
              (mock-env {"MILVUS_HOST"       "milvus.svc"
                         "MILVUS_PORT"       "9091"
                         "MILVUS_TRANSPORT"  "http"
                         "MILVUS_TOKEN"      "secret"
                         "MILVUS_DATABASE"   "vectors"
                         "MILVUS_SECURE"     "true"}))]
      (is (r/ok? r))
      (let [v (:ok r)]
        (is (= "milvus.svc"   (:host v)))
        (is (= 9091           (:port v))      "port coerces string→int")
        (is (= :http          (:transport v)) "transport coerces string→keyword")
        (is (= "secret"       (:token v)))
        (is (= "vectors"      (:database v)))
        (is (= true           (:secure v))    "secure coerces \"true\"→true")))))

(deftest blank-env-falls-back-to-default
  (testing "MILVUS_HOST=\"\" triggers default, not silent empty string"
    (let [r (config/resolve-MilvusConfig {} (mock-env {"MILVUS_HOST" ""}))]
      (is (r/ok? r))
      (is (= "localhost" (:host (:ok r)))))))

(deftest manifest-overrides-win
  (testing "explicit overrides map beats env (manifest config takes precedence)"
    (let [r (config/resolve-MilvusConfig
              {:host "from-manifest" :port 7777}
              (mock-env {"MILVUS_HOST" "from-env" "MILVUS_PORT" "9999"}))]
      (is (r/ok? r))
      (let [v (:ok r)]
        (is (= "from-manifest" (:host v)))
        (is (= 7777            (:port v)))))))

(deftest invalid-port-collects-error
  (testing "non-numeric MILVUS_PORT collects coercion error, not throws"
    (let [r (config/resolve-MilvusConfig {} (mock-env {"MILVUS_PORT" "not-a-number"}))]
      (is (r/err? r))
      (is (= :config/resolution-failed (:error r)))
      (is (some #(= :port (:field %)) (:errors r))))))

(deftest token-optional
  (testing ":token has :required false — nil resolves OK"
    (let [r (config/resolve-MilvusConfig {} (mock-env {}))]
      (is (r/ok? r))
      (is (nil? (:token (:ok r)))))))
