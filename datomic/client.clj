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

(ns datomic.client
  "Client library for interacting with Datomic.
  Alpha, subject to change.

  Functions that interact with the cluster are asynchronous and return
  a core.async channel. In the case of an error, an error map is
  returned which will return true from 'error?'

  All async, channel-returning functions in this namespace
  allow an optional :timeout argument in their arg-map.

  A database is a descriptor map with possible keys:
  :database-id, :t, :next-t, :as-of, :since, :history
  Please use the various functions that produce database values
  and do not construct these maps directly.

  Functions that return chunked results will return a succession of
  vectors of values in a channel. The channel will be closed when the
  results are exhausted.  If there is an error it will be in the
  channel instead of the chunk.

  Functions that return datoms return values of a type that supports
  indexed (count/nth) access of [e a v t added] as well as
  lookup (keyword) access via :e :a :v :t :added."
  (:refer-clojure :exclude [chunk sync])
  (:require [clojure.core.async :as a :refer (>! <! close! go promise-chan put! take!)]
            [datomic.client.config :as config]
            [datomic.client.conn-cache :as cache]
            [datomic.client.conn-impl :as ci]
            [datomic.client.impl.protocols :as p]
            [datomic.client.impl.types :as types])
  (:import [java.util UUID]))

(set! *warn-on-reflection* true)

;;; API

(defn as-of
  "Returns the value of the database as of some point t, inclusive. t
  can be a transaction number, transaction ID, or Date.

  Alpha, subject to change."
  [db t]
  (assoc db :as-of t))

(defn error?
  "Returns true if resp is an error map. Use
:cognitect.anomalies/category key to get more info.

  Alpha, subject to change."
  [resp]
  (boolean (:cognitect.anomalies/category resp)))

(defn ^:skip-wiki handle-response
  [c retc f]
  (take! c
         (fn [resp]
           (try
            (if-let [x (or (when (error? resp) resp)
                           (f resp))]
              (put! retc x)
              (close! retc))
            (catch Throwable t
              (put! retc (ci/throwable->anomaly t)))))))

(defn ^:skip-wiki handle-chunked-responses
  [conn-impl c retc f]
  (go
   (let [resp (<! c)]
     (try
       (when-let [x (or (when (error? resp) resp)
                        (f resp))]
         (>! retc x)
         (if (:next-offset resp)
           (let [{:keys [next-offset next-token chunk]} resp
                 nreq {:op :next
                       :next-token next-token
                       :offset next-offset
                       :chunk chunk}
                 ch    (ci/queue-request conn-impl nreq)]
             (handle-chunked-responses conn-impl ch retc f))
           (close! retc)))
       (catch Throwable t
         (put! retc (ci/throwable->anomaly t))
         (close! retc))))))


(def ^:skip-wiki PRO_ACCOUNT ":account-id value for connecting to Datomic Pro's peer server"
  (str (java.util.UUID. 0 0)))
(def ^:skip-wiki PRO_REGION ":region value for connecting to Datomic Pro's peer server"
  "none")

(defn- invalid-name-result
  [arg-map]
  (when-not (:tx-data arg-map)
    (ci/result-chan
     {:cognitect.anomalies/category :cognitect.anomalies/incorrect
      :cognitect.anomalies/message "Transaction arg map must contain :tx-data"})))

(defn- no-conn-result
  []
  (ci/result-chan
   {:cognitect.anomalies/category :cognitect.anomalies/incorrect
    :cognitect.anomalies/message "No connection available."}))

(defn- invalid-connect-args-result
  [arg-map]
  (when-not (:db-name arg-map)
    (ci/result-chan
     {:cognitect.anomalies/category :cognitect.anomalies/incorrect
      :cognitect.anomalies/message "Arg missing db-name"
      ::arg arg-map})))

(defn connect
  "Async, see also namespace doc.
  Connects to a database described by the argument map, which
  has the following keys:

  :db-name - a string which names a database.
  :timeout - optional, timeout value to use for all remote requests

  There are six additional optional keys:

  :account-id - must be set to datomic.client/PRO_ACCOUNT
  :access-key - access key associated with this account
  :secret - secret associated with this account
  :endpoint - host and port number for peer-server
  :region - must be \"none\"
  :service - must be \"peer-server\"

  The access-key/secret pair must match with one used to launch peer-server.

  If the optional keys are not specified, they are read from environment
  variables DATOMIC_ACCOUNT_ID, DATOMIC_SECRET, DATOMIC_ENDPOINT, DATOMIC_REGION,
  and DATOMIC_SERVICE, respectively, then from the ~/.datomic/config file.

  Returns a promise channel that will yield a connection object.

  Datomic connections do not adhere to an acquire/use/release
  pattern. They are thread-safe and long lived. Connections are cached
  such that calling connect multiple times with the same database
  value will return the same connection object.

  Alpha, subject to change."
  [arg-map]
  (let [{:keys [db-name account-id access-key secret endpoint service region] :as config} (-> arg-map config/config config/validate)]
    (or (ci/error-result config)
        (invalid-connect-args-result arg-map)
        (let [acct-db (str account-id "." db-name)
              retc (promise-chan)]
          (if-let [conn (cache/config->conn config)]
            (put! retc conn)
            (let [conn-impl (ci/create config)
                  ch (ci/queue-request conn-impl {:op :datomic.catalog/resolve-db
                                                  :db-name db-name})]
              (take! ch
                     (fn [{:keys [database-id] :as resolved}]
                       (try
                        (if-not (error? resolved)
                          (let [state (atom {:t 0 :next-t 0})
                                conn-impl' (assoc conn-impl
                                             :state state
                                             :database-id database-id)
                                ch' (ci/queue-request conn-impl' {:op :status})]
                            (handle-response ch' retc
                                             (fn [{:keys [t next-t] :as resp}]
                                               (swap! state ci/advance-t {:t t :next-t next-t})
                                               (let [conn (types/connection config
                                                                            conn-impl'
                                                                            account-id
                                                                            db-name
                                                                            database-id
                                                                            state)]
                                                 (cache/put config database-id conn)
                                                 conn))))
                          (put! retc resolved))
                        (catch Throwable t
                          (put! retc (ci/throwable->anomaly t))))))))
          retc))))

(def ^:private index->idxvec
  {:eavt [:e :a :v :t]
   :aevt [:a :e :v :t]
   :avet [:a :v :e :t]
   :vaet [:v :a :e :t]})

(defn- with-db? [db]
  (:next-token db))

(defn- db->conn-impl
  [db]
  (some-> db :database-id cache/database-id->conn p/conn-impl))

(defn datoms
  "Async and chunked, see also namespace doc.
   Returns datoms from an index as specified by arg-map:

  :index - One of :eavt, :aevt, :avet, or :vaet, indicating the
    desired index. EAVT, AEVT, and AVET indexes contain all datoms.
    VAET contains only datoms for attributes of :db.type/ref.
  :components - Optional. A vector in the same order as the index
    containing one or more values to further narrow the result
  :offset - Optional. Number of results to omit from the beginning of
    the returned data.
  :limit - Optional. Maximum total number of results that will be
    returned. Specify -1 to indicate no limit. Defaults to 1000.
  :chunk - Optional. Maximum number of results that will be returned
    for each chunk, up to 10000. Defaults to 1000.

  Returns a channel which yields chunks of datoms.

  Alpha, subject to change."
  [db {:keys [index components] :as arg-map}]
  (if-let [conn-impl (db->conn-impl db)]
    (let [retc (a/chan)
          req  (merge db
                      (zipmap (index->idxvec index) components)
                      {:offset 0}
                      (dissoc arg-map :components)
                      {:op :datoms})
          ch    (ci/queue-request conn-impl req)]
      (handle-chunked-responses conn-impl ch retc :data)
      retc)
    (no-conn-result)))

(defn db
  "Returns the current value of the database.

  Alpha, subject to change."
  [conn]
  (if (types/connection? conn)
    (let [st @(p/state conn)]
      {:database-id (p/db-id conn)
       :t (:t st)
       :next-t (:next-t st)})
    {:cognitect.anomalies/category :cognitect.anomalies/incorrect
     :cognitect.anomalies/message "Not a connection"}))

(defn history
  "Returns a database value containing all assertions and
  retractions across time. A history database can be used for
  datoms and index-range calls and queries, but not for with calls.
  Note that queries against a history database will include
  retractions as well as assertions. These can be distinguished
  by the fifth datom field ':added', which is true for asserts
  and false for retracts.

  Alpha, subject to change."
  [db]
  (assoc db :history true))

(defn index-range
  "Async and chunked, see also namespace doc.
  Returns datoms from the AVET index as specified by arg-map:

  :attrid - An attribute keyword or ID.
  :start - Optional. The start point, inclusive, of the requested
    range (as a transaction number, transaction ID, or Date) or
    nil/absent to start from the beginning.
  :end - Optional. The end point, exclusive, of the requested range (as a
    transaction number, transaction ID, or Date) or nil/absent to return
    results through the end of the attribute index.
  :offset - Optional. Number of results to omit from the beginning of
    the returned data.
  :limit - Optional. Maximum total number of results that will be
    returned. Specify -1 to indicate no limit. Defaults to 1000.
  :chunk - Maximum number of results that will be returned for each
    chunk, up to 10000. Defaults to 1000.

  Returns a channel which yields chunks of datoms.

  Alpha, subject to change."
  [db arg-map]
  (if-let [conn-impl (db->conn-impl db)]
    (let [retc (a/chan)
          req  (cond->
                (merge
                 db
                 {:offset 0}
                 arg-map
                 {:op :index-range}))
          ch    (ci/queue-request conn-impl req)]
      (handle-chunked-responses conn-impl ch retc :data)
      retc)
    (no-conn-result)))

(defn pull
  "Async, see also namespace doc.

  Returns a promise channel with a hierarchical selection
  described by arg-map:

  :selector - the selector expression
  :eid - an entity id

  Alpha, subject to change."
  [db arg-map]
  (if-let [conn-impl (db->conn-impl db)]
    (let [retc (promise-chan)
          req  (merge db arg-map {:op :pull})
          ch    (ci/queue-request conn-impl req)]
      (handle-response ch retc #(or (:result %) {}))
      retc)
    (no-conn-result)))

(defn db-stats
  "Async, see also namespace doc.

  Queries for database stats. Returns a promise channel with a map
  including at least:

  :datoms - total count of datoms in the database, including history.

  Alpha, subject to change."
  [db]
  (if-let [conn-impl (db->conn-impl db)]
    (let [retc (promise-chan)
          req  (merge db {:op :db-stats})
          ch    (ci/queue-request conn-impl req)]
      (handle-response ch retc :result)
      retc)
    (no-conn-result)))

(defn log
  "Given a connection, returns a log descriptor usable in query.

  Alpha, subject to change."
  [conn]
  {:log (p/db-id conn)})

(defn q
  "Async and chunked, see also namespace doc.

  Performs the query described by arg-map:

  :query - The query to perform. A map, list, or string (see below).
  :args - data sources for the query, e.g. a database value
    retrieved from a call to db, a list of lists, and/or rules.
  :offset - Optional. Number of results to omit from the beginning of
    the returned data.
  :limit - Optional. Maximum total number of results that will be
    returned. Specify -1 to indicate no limit. Defaults to 1000.
  :chunk - Optional. Maximum number of results that will be returned
    for each chunk, up to 10000. Defaults to 1000.
  :timeout - Optional. Amount of time in milliseconds after which
    the query may be cancelled. Defaults to 60000.

  :args are data sources i.e. a database value retrieved from
  a call to db, a list of lists, and/or rules. If only one data
  source is provided, no :in section is required, else the :in section
  describes the inputs.

  :query can be a map, list, or string:

  The query map form is {:find vars-and-aggregates
                         :with vars-included-but-not-returned
                         :in sources
                         :where clauses}
  where vars, sources and clauses are lists.

  :with is optional, and names vars to be kept in the aggregation set but
  not returned.

  The query list form is [:find ?var1 ?var2 ...
                          :with ?var3 ...
                          :in $src1 $src2 ...
                          :where clause1 clause2 ...]
  The query list form is converted into the map form internally.

  The query string form is a string which, when read, results in a
  query list form or query map form.

  Query parse results are cached.

  Returns a channel that yields chunked query result tuples.

  Alpha, subject to change."
  [conn arg-map]
  (let [retc (a/chan)
        req  (merge {:timeout 60000 :offset 0}
                    (assoc arg-map :op :q))
        conn-impl (p/conn-impl conn)
        ch    (ci/queue-request conn-impl req)]
    (handle-chunked-responses conn-impl ch retc :data)
    retc))

(defn shutdown
  "Shuts down this connection, releasing any resources that might be
  held open. This is an optional method, and callers are not expected
  to call it, but can if they want to explicitly release any open
  resources. Once a client has been shutdown, it should not be used to
  make any more requests.

  Alpha, subject to change."
  [conn]
  (cache/forget-conn conn)
  nil)

(defn since
  "Returns the value of the database since some point t, exclusive.
  t is a transaction number, transaction ID, or Date.

  Alpha, subject to change."
  [db t]
  (assoc db :since t))

(defn tx-range
  "Async and chunked, see also namespace doc.

  Retrieve a range of transactions in the log as specified by arg-map:

  :start - Optional. The start point, inclusive, of the requested
    range (as a transaction number, transaction ID, or Date) or nil to
    start from the beginning of the transaction log.
  :end - Optional. The end point, exclusive, of the requested
    range (as a transaction number, transaction ID, or Date) or nil to
    return results through the end of the transaction log.
  :offset - Optional. Number of results to omit from the beginning of
    the returned data.
  :limit - Optional. Maximum total number of results that will be
    returned. Specify -1 to indicate no limit. Defaults to 1000.
  :chunk - Optional. Maximum number of results that will be returned
    for each chunk, up to 10000. Defaults to 1000.

  Returns a channel which yields chunked transactions.
  Transactions are maps with keys:

  :t - the T point of the transaction
  :tx-data - a collection of the datoms asserted/retracted by the
    transaction

  Alpha, subject to change."
  [conn arg-map]
  (let [retc (a/chan)
        req  (merge {:op :tx-range
                     :database-id (p/db-id conn)
                     :offset 0}
                    arg-map)
        conn-impl (p/conn-impl conn)
        ch    (ci/queue-request conn-impl req)]
    (handle-chunked-responses conn-impl ch retc :data)
    retc))

(defn transact
  "Async, see also namespace doc.

  Submits a transaction specified by arg-map:

  :tx-data - A list of write operations, each of which is either an
    assertion, a retraction or the invocation of a data function. Each
    nested list starts with a keyword identifying the operation followed
    by the arguments for the operation. Write operations may also be
    maps from attribute identifiers to values to be asserted.

  Returns a promise channel that can be used to monitor the
  completion of the transaction. See (doc datomic.client). If the
  transaction commits, the channel's value is a map with the following
  keys:

  :db-before - Database value before the transaction
  :db-after - Database value after the transaction
  :tx-data - Collection of Datoms produced by the transaction
  :tempids - A map from tempids to their resolved entity IDs.

  Alpha, subject to change."
  [conn arg-map]
  (or (invalid-name-result arg-map)
      (let [retc (promise-chan)
            db-id (p/db-id conn)
            req   (-> {:op :transact
                       :tx-id (UUID/randomUUID)
                       :database-id db-id}
                      (merge arg-map))
            conn-impl (p/conn-impl conn)
            ch     (ci/queue-request conn-impl req)]
        (handle-response ch retc
          (fn [resp]
            (select-keys resp [:db-before :db-after :tx-data :tempids])))
        retc)))

(defn with-db
  "Async, see also namespace doc.

  Returns a promise channel that yields a database value suitable
for passing to 'with'.

  Alpha, subject to change."
  [conn]
  (if-let [conn-impl (p/conn-impl conn)]
    (let [retc (promise-chan)
          st        @(p/state conn)
          db-id     (p/db-id conn)
          req       {:op :with-db
                     :database-id db-id
                     :t (:t st)}
          ch (ci/queue-request conn-impl req)]
      (handle-response ch retc #(select-keys % [:database-id :t :next-t :next-token]))
      retc)
    (no-conn-result)))

(defn with
  "Async, see also namespace doc.

  Applies tx-data to a database that must have been returned from
  'with-db' or a prior call to 'with'.  The result of calling 'with'
  is as if the data was applied in a transaction, but the source of
  the database is unaffected.

  Takes data in the same format expected by transact, and returns a
  promise channel similar to the return of transact.

  Alpha, subject to change."
  [db arg-map]
  (assert (with-db? db)
    "Can only call with on a database returned from with-db")
  (if-let [conn-impl (db->conn-impl db)]
    (let [retc (promise-chan)
          req  (cond->
                (merge db
                       arg-map
                       {:op :with}))
          ch    (ci/queue-request conn-impl req)]
      (handle-response ch retc #(select-keys % [:db-after :tempids :db-before :tx-data]))
      retc)
    (no-conn-result)))


