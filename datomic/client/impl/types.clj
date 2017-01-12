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

(ns ^:skip-wiki datomic.client.impl.types
  (:refer-clojure :exclude [proxy])
  (:require [clojure.core.async :as async]
            [datomic.client.impl.protocols :as p])
  (:import [java.util UUID]
           [java.util.concurrent Executors]
           [clojure.lang ILookup Indexed Counted]))

(set! *warn-on-reflection* true)

;; -----------------------------------------------------------------------------
;; Connection

(deftype Connection [config conn-impl account-id db-name db-id state]
  Object
  (toString
   [_]
   (str "#datomic.client.impl.types.Connection["
        (pr-str (merge @state {:account-id account-id :db-name db-name
                               :db-id db-id :timeout (:timeout conn-impl)}))
        "]"))
  p/IState
  (state [this] state)
  p/IConnection
  (account-id [this] account-id)
  (db-name [this] db-name)
  (db-id [this] db-id)
  p/IConnectionImpl
  (conn-impl [this] conn-impl)
  ILookup
  (valAt [this k]
    (.valAt this k nil))
  (valAt [this k notFound]
    (case k
      :config  config
      :conn-impl conn-impl
      :db-name db-name
      :db-id   db-id
      :state   state
      notFound)))

(defn connection
  [config conn-impl account-id db-name db-id state]
  (Connection. config (assoc conn-impl :state state) account-id db-name db-id state))

(defn connection? [x]
  (instance? Connection x))

;; -----------------------------------------------------------------------------
;; Datom

(deftype Datom [e a v t added]
  Object
  (equals [this o]
          (or (identical? this o)
              (let [^Datom o o]
                (and (instance? Datom o)
                     (= t (:t o))
                     (= e (:e o))
                     (= a (:a o))
                     (zero? (compare v (:v o)))
                     (= added (:added o))))))
  (hashCode [_]
            (int (-> (hash e) (bit-xor (hash a)) (bit-xor (hash v)) (bit-xor (hash added)))))

  ILookup
  (valAt [this k]
         (.valAt this k nil))
  (valAt [this k notFound]
         (case k
               :e e
               :a a
               :v v
               :t t
               :added added
               notFound))

  Counted
  (count [_] 5)
    
  Indexed
  (nth [this i]
       (if (<= 0 i 4)
         (.nth this i nil)
         (throw (IndexOutOfBoundsException.))))
  (nth [this i notFound]
       (case i
             0 e
             1 a
             2 v
             3 t
             4 added
             notFound)))

(defn datom [e a v t added]
  (Datom. e a v t added))

(defn datom? [x]
  (instance? Datom x))

(defmethod print-method Datom
  [^Datom d ^java.io.Writer w]
  (.write w "#datom[") (print-method (:e d) w)
  (.write w " ") (print-method (:a d) w)
  (.write w " ") (print-method (:v d) w)
  (.write w " ") (print-method (:t d) w)
  (.write w " ") (print-method (:added d) w)
  (.write w "]"))
