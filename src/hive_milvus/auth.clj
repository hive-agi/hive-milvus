(ns hive-milvus.auth
  "Effective Milvus auth token from resolved addon config.

   Milvus with `authorizationEnabled` answers 401 to any request without
   a bearer token. The token was only ever read from MILVUS_TOKEN, so a
   server launched without that env var (any launcher but the blw
   wrapper) ran unauthenticated and every memory read failed.

   `effective-token` closes that gap:
     1. an explicit :token wins (plain or \"pass:<path>\");
     2. else :user + :password compose \"user:password\", the Milvus
        basic-auth bearer form (password plain or \"pass:<path>\").
   Secrets never enter logs or error messages."
  (:require [clojure.java.shell :as shell]
            [clojure.string :as str]
            [malli.core :as m]))

(defn- pass-show
  "First line of `pass show <path>`, or nil when pass fails."
  [path]
  (try
    (let [{:keys [exit out]} (shell/sh "pass" "show" path)]
      (when (zero? exit)
        (some-> out str/split-lines first str/trim not-empty)))
    (catch Throwable _ nil)))

(defn resolve-secret
  "Plain string as-is, \"pass:<path>\" through password-store, blank → nil."
  ([v] (resolve-secret v pass-show))
  ([v pass-fn]
   (cond
     (not (string? v))             nil
     (str/blank? v)                nil
     (str/starts-with? v "pass:")  (pass-fn (subs v 5))
     :else                         v)))

(defn effective-token
  "Bearer token for `cfg`, or nil when no credentials are configured."
  ([cfg] (effective-token cfg pass-show))
  ([{:keys [token user password]} pass-fn]
   (or (resolve-secret token pass-fn)
       (when-let [u (resolve-secret user pass-fn)]
         (when-let [p (resolve-secret password pass-fn)]
           (str u ":" p))))))

(defn with-effective-token
  "`cfg` with :token set to the effective token and :password dropped,
   so the raw password does not travel further than this namespace."
  ([cfg] (with-effective-token cfg pass-show))
  ([cfg pass-fn]
   (let [t (effective-token cfg pass-fn)]
     (cond-> (dissoc cfg :password)
       t (assoc :token t)))))

(m/=> resolve-secret [:function
                      [:=> [:cat :any] [:maybe :string]]
                      [:=> [:cat :any fn?] [:maybe :string]]])
(m/=> effective-token [:function
                       [:=> [:cat :map] [:maybe :string]]
                       [:=> [:cat :map fn?] [:maybe :string]]])
(m/=> with-effective-token [:function
                            [:=> [:cat :map] :map]
                            [:=> [:cat :map fn?] :map]])
