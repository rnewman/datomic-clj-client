;; Copyright (c) Cognitect, Inc.
;; All rights reserved.

;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;;      http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS-IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns ^:skip-wiki datomic.client.config
    (:require [clojure.java.io :as io]
              [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defn- ^:skip-wiki parse-config-file
  "Returns a map from config file."
  [s]
  (try
   (when (and s (.exists (io/file s)))
     (letfn [(parse-kv [kv-str]
                       (let [i (str/index-of kv-str \=)
                             k (str/trim (subs kv-str 0 i))
                             v (str/trim (subs kv-str (inc i)))]
                         [(keyword k) v]))]
       (->> (-> (slurp s) str/trim (str/split #"\n"))
            (map parse-kv)
            (into {}))))
   (catch Exception e
     (binding [*out* *err*]
       (println "Unable to parse" s)
       nil))))

(defn- ^:skip-wiki default-config [] {:timeout 60000})

(defn- ^:skip-wiki home-config
  "Returns config derived from .datomic/config, or anomaly."
  []
  (parse-config-file (io/file (System/getProperty "user.home") ".datomic/config")))

(defn- ^:skip-wiki env-config
  "Returns config from envars."
  []
  {:account-id (System/getenv "DATOMIC_ACCOUNT_ID")
   :access-key (System/getenv "DATOMIC_ACCESS_KEY")
   :secret     (System/getenv "DATOMIC_SECRET")
   :endpoint   (System/getenv "DATOMIC_ENDPOINT")
   :service    (System/getenv "DATOMIC_SERVICE")
   :region     (System/getenv "DATOMIC_REGION")})

(defn- valid?
  [config]
  (let [{:keys [account-id access-key secret endpoint service region]} config]
    (and (string? account-id) (string? access-key) (string? secret) (string? endpoint)
         (string? service) (string? region))))

(defn ^:skip-wiki config
  "Applies precedence rules across all config sources, returns config"
  [args]
  (let [merge-cfg (fn [& args]
                    (apply merge-with (fn [old new] (or new old)) args))
        cfg (merge-cfg (default-config) (env-config) args)]
    (if (valid? cfg)
      cfg
      (merge-cfg (home-config) cfg))))

(defn ^:skip-wiki validate
  "Returns a config map or a :cognitect/anomaly map"
  [config]
  (if (valid? config)
    config
    {:cognitect.anomalies/category :cognitect.anomalies/incorrect
     :cognitect.anomalies/message (str "Incomplete or invalid connection config: " config)}))


