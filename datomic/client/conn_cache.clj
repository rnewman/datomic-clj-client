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

(ns ^:skip-wiki datomic.client.conn-cache)

(defonce cache-ref (atom {}))

(defn config->database-id
  [config]
  (get-in @cache-ref [:config->database-id config]))

(defn config->conn
  [config]
  (let [cache @cache-ref]
    (when-let [database-id (get-in cache [:config->database-id config])]
      (get-in cache [:database-id->conn database-id]))))

(defn database-id->conn
  [database-id]
  (get-in @cache-ref [:database-id->conn database-id]))

(defn put
  [config database-id conn]
  (swap! cache-ref (fn [cache]
                 (-> cache
                     (assoc-in [:config->database-id config] database-id)
                     (assoc-in [:database-id->conn database-id] conn)
                     (assoc-in [:conn->config conn] config)))))

(defn forget-conn
  [conn]
  (swap! cache-ref (fn [cache]
                     (if-let [config (get-in cache [:conn->config conn])]
                       (if-let [database-id (get-in cache [:config->database-id config])]
                         (-> cache
                             (update :config->database-id dissoc config)
                             (update :database-id->conn dissoc database-id)
                             (update :conn->config dissoc conn))
                         cache)
                       cache))))

(defn forget-config
  [config]
  (swap! cache-ref (fn [cache]
                     (if-let [database-id (get-in cache [:config->database-id config])]
                       (if-let [conn (get-in cache [:database-id->conn database-id])]
                         (-> cache
                             (update :config->database-id dissoc config)
                             (update :database-id->conn dissoc database-id)
                             (update :conn->config dissoc conn))
                         cache)
                       cache))))


