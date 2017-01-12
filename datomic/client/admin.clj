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

(ns datomic.client.admin
  "Client library for admin interactions with Datomic.
Alpha, subject to change."
  (:refer-clojure :exclude [chunk sync])
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [datomic.client :as client]
            [datomic.client.config :as config]
            [datomic.client.conn-cache :as cache]
            [datomic.client.conn-impl :as ci]
            [datomic.client.impl.protocols :as p]
            [datomic.client.impl.types :as types])
  (:import [java.util UUID]))

(set! *warn-on-reflection* true)

(defn- invalid-name-result
  [db-name]
  (when-not db-name
    (ci/result-chan
     {:cognitect.anomalies/category :cognitect.anomalies/incorrect
      :cognitect.anomalies/message (str ":db-name must be a string, got [" db-name "] instead")})))

(defn create-database
  "Async, see also datomic.client namespace doc.

  Creates a database described by the arg-map:

  :db-name - a string which names a database.

  The arg-map can also contain any of the optional keys listed
  for datomic.client/connect.

  Returns a promise channel that will yield true.

  Alpha, subject to change."
  [arg-map]
  (let [{:keys [db-name] :as config} (-> arg-map config/config config/validate)]
    (or (ci/error-result config)
        (invalid-name-result db-name)
        (let [retc (async/promise-chan)
              conn-impl (ci/create (dissoc config :db-name))
              ch (ci/queue-request conn-impl {:op :datomic.catalog/create-db
                                              :db-name db-name})]
          (client/handle-response ch retc (constantly true))
          retc))))

(defn delete-database
  "Async, see also datomic.client namespace doc.

  :db-name - a string which names a database.

  The arg-map can also contain any of the optional keys listed
  for datomic.client/connect.

  Returns a promise channel that will yield true.

  Alpha, subject to change."
  [arg-map]
  (let [{:keys [db-name account-id] :as config} (-> arg-map config/config config/validate)]
    (or (ci/error-result config)
        (invalid-name-result db-name)
        (let [acct-db (str account-id "." db-name)
              retc (async/promise-chan)
              conn-impl (ci/create (dissoc config :db-name))
              ch (ci/queue-request conn-impl {:op :datomic.catalog/delete-db
                                              :db-name db-name})]
          (cache/forget-config config)
          (client/handle-response ch retc (constantly true))
          retc))))

(defn list-databases
  "Async, see also datomic.client namespace doc.
  Lists all databases for the account specified by the arg-map, which
  requires no keys but can contain any of the optional keys listed for
  datomic.client/connect

  Returns a promise-channel that will yield a collection of database
  names.

  Alpha, subject to change."
  [arg-map]
  (let [config (-> arg-map config/config config/validate)]
    (or
     (ci/error-result config)
     (let [retc (async/promise-chan)
           conn-impl (ci/create (dissoc config :db-name))
           ch (ci/queue-request conn-impl {:op :datomic.catalog/list-dbs})]
       (client/handle-response ch retc :result)
       retc))))
