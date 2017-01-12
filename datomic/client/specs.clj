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

(ns datomic.client.specs
  (:require [clojure.core.async :as async]
            [clojure.spec :as s]
            [cognitect.anomalies :as anom]
            [datomic.client :as c]
            [datomic.client.admin]
            datomic.client.impl.types)
  (:import [clojure.core.async.impl.protocols Channel]
           [datomic.client.impl.types Connection]))

;; -------------------------------------------------------------------------------
;; DATOMIC SPECS

;; ? explore custom generator if you are recursive
(s/def :datomic/lookup-ref
  (s/tuple :datomic/a
           :datomic/v))

(s/def :datomic/e
  (s/or :eid nat-int?
        :ident keyword?
        :lookup-ref :datomic/lookup-ref))

(s/def :datomic/a :datomic/e)

(s/def :datomic.db.type/string string?)
(s/def :datomic.db.type/boolean boolean?)
(s/def :datomic.db.type/long int?)
;;(s/def :datomic.db.type/bigint bigint?)
(s/def :datomic.db.type/keyword keyword?)
(s/def :datomic.db.type/float float?)
(s/def :datomic.db.type/double double?)
(s/def :datomic.db.type/bigdec bigdec?)
(s/def :datomic.db.type/ref int?)
(s/def :datomic.db.type/instant inst?)
(s/def :datomic.db.type/uuid uuid?)
(s/def :datomic.db.type/uri uri?)
(s/def :datomic.db.type/bytes bytes?)

(s/def :datomic/v
  (s/or :string :datomic.db.type/string
        :boolean :datomic.db.type/boolean
        :long :datomic.db.type/long
        :keyword :datomic.db.type/keyword
        ;;:bigint :datomic.db.type/bigint
        :float :datomic.db.type/float
        :double :datomic.db.type/double
        :bigdec :datomic.db.type/bigdec
        :ref :datomic.db.type/ref
        :instant :datomic.db.type/instant
        :uuid :datomic.db.type/uuid
        :uri :datomic.db.type/uri
        :bytes :datomic.db.type/bytes))

(s/def :datomic/t pos-int?)
(s/def :datomic/history boolean?)

;; ident?
(s/def :datomic/tx
  (s/or :t-or-tx-entity-id  pos-int?
        :inst inst?))

(s/def :datomic/as-of :datomic/tx)

;; -------------------------------------------------------------------------------
;; CLIENT API SPECS

(s/def :datomic.client/database (s/keys :req-un [:datomic.client.protocol/database-id
                                                 :datomic.client.protocol/t
                                                 :datomic.client.protocol/next-t]))
(s/def :datomic.client/with-database (s/merge :datomic.client/database
                                              (s/keys :req-un [:datomic.client.protocol/next-token])))

(defn connection?
  [x]
  (instance? Connection x))
(s/def :datomic.client/connection connection?)

(defn channel?
  [x]
  (instance? Channel x))

; -------------------------------------------------------------------------------
;; client config

(s/def :datomic.client/account-id string?)
(s/def :datomic.client/db-name string?)
(s/def :datomic.client/region string?)
(s/def :datomic.client/timeout pos-int?)

(s/def :datomic.client/connection-config
  (s/keys :req-un [:datomic.client/db-name]
          :opt-un [:datomic.client/region :datomic.client/timeout :datomic.client/account-id]))

;; -------------------------------------------------------------------------------
;; client read args and return value

(s/def :datomic.client.read/args
  (s/keys :opt-un [:datomic.client.protocol/offset
                   :datomic.client.protocol/limit
                   :datomic.client.protocol/chunk]))

(s/def :datomic.client/init any?)

;; -------------------------------------------------------------------------------
;; API

(s/fdef datomic.client/as-of
        :args (s/cat :db :datomic.client/database :datomic.client.protocol/t :datomic/tx)
        :ret (s/merge :datomic.client/database
                      (s/keys :req-un [:datomic/as-of])))

(s/fdef datomic.client/connect
        :args (s/cat :conn-config :datomic.client/connection-config)
        :ret channel?)

;; TBD: components must be valid eavt values
;; in order corresponding to specified index
;; ? switch to every? or coll-of?
;; ? could we make components a map since we
;; use one for args anyway
;; ? could we use spec to conform input arguments
;; to get wire protocol map?
(s/def :datomic.client/components
  (s/and vector? #(< (count %) 4)))

(s/def :datomic.client.datoms/eavt
  (s/? (s/cat :e :datomic/e
              :more (s/? (s/cat :a :datomic/a
                                :more (s/? (s/cat :v :datomic/v
                                                  :more (s/? :datomic/t))))))))

(s/def :datomic.client.datoms/aevt
  (s/? (s/cat :a :datomic/a
              :more (s/? (s/cat :e :datomic/e
                                :more (s/? (s/cat :v :datomic/v
                                                  :more (s/? :datomic/t))))))))

(s/def :datomic.client.datoms/avet
  (s/? (s/cat :a :datomic/a
              :more (s/? (s/cat :v :datomic/v
                                :more (s/? (s/cat :e :datomic/e
                                                  :more (s/? :datomic/t))))))))

(s/def :datomic.client.datoms/vaet
  (s/? (s/cat :v :datomic/v
              :more (s/? (s/cat :a :datomic/a
                                :more (s/? (s/cat :e :datomic/e
                                                  :more (s/? :datomic/t))))))))

(s/def :datomic.client.datoms/arg-map
  (s/and
    (s/merge (s/keys
               :req-un [:datomic.client.protocol/index]
               :opt-un [:datomic.client/components])
             :datomic.client.read/args)
    (fn [{:keys [index components]}]
      ;; depending on index value, components must conform
      (case index
        :eavt (s/valid? :datomic.client.datoms/eavt components)
        :aevt (s/valid? :datomic.client.datoms/aevt components)
        :avet (s/valid? :datomic.client.datoms/avet components)
        :vaet (s/valid? :datomic.client.datoms/vaet components)))))

(s/def :datomic.client.datoms/data vector?)
(s/def :datomic.client.tx-range/data vector?)
(s/def :datomic.client.index-range/data vector?)
(s/def :datomic.client/query-result vector?)

(s/fdef datomic.client/datoms
        :args (s/cat :db :datomic.client/database
                     :arg-map :datomic.client.datoms/arg-map)
        :ret channel?)

(s/fdef datomic.client/db
        :args (s/cat :conn :datomic.client/connection)
        :ret :datomic.client/database)

(s/fdef datomic.client/history
        :args (s/cat :db :datomic.client/database)
        :ret (s/merge :datomic.client/database
                      (s/keys :req-un [:datomic/history])))

(s/def :datomic.client.index-range/attrid :datomic/a)
(s/def :datomic.client.index-range/start :datomic/v)
(s/def :datomic.client.index-range/end :datomic/v)

(s/def :datomic.client.index-range/arg-map
  (s/merge (s/keys
             :req-un [:datomic.client.index-range/attrid]
             :opt-un [:datomic.client.index-range/start
                      :datomic.client.index-range/end])
           :datomic.client.read/args))

(s/fdef datomic.client/index-range
        :args (s/cat :db :datomic.client/database
                     :arg-map :datomic.client.index-range/arg-map)
        :ret channel?)

(s/def :datomic.client.pull/selector vector?)
(s/def :datomic.client.pull/eid :datomic/e)
(s/def :datomic.client.pull/result map?)

(s/fdef datomic.client/pull
        :args (s/cat :db :datomic.client/database
                     :arg-map (s/keys :req-un [:datomic.client.pull/selector
                                               :datomic.client.pull/eid]))
        :ret channel?)

(s/def :datomic.client.q/args (s/coll-of any?))
(s/def :datomic.client.q/query
  (s/or :map map?
        :list vector?
        :string string?))

(s/def :datomic.client.q/arg-map
  (s/merge (s/keys :req-un [:datomic.client.q/query
                            :datomic.client.q/args]
                   :opt-un [:datomic.client/timeout])
           :datomic.client.read/args))

(s/fdef datomic.client/q
        :args (s/cat :conn :datomic.client/connection
                     :arg-map :datomic.client.q/arg-map)
        :ret channel?)

(s/fdef datomic.client/shutdown
        :args (s/cat :conn :datomic.client/connection)
        :ret nil?)

(s/fdef datomic.client/since
        :args (s/cat :db :datomic.client/database :t :datomic/t)
        :ret :datomic.client/database)

(s/def :datomic.client.tx-range/start :datomic/tx)
(s/def :datomic.client.tx-range/end :datomic/tx)

(s/def :datomic.client.tx-range/arg-map
  (s/merge (s/keys :opt-un [:datomic.client.tx-range/start
                            :datomic.client.tx-range/end])
           :datomic.client.read/args))

(s/fdef datomic.client/tx-range
        :args (s/cat :conn :datomic.client/connection
                     :arg-map :datomic.client.tx-range/arg-map)
        :ret channel?)

(s/def :datomic.client.transact/tx-data (s/coll-of any?))
(s/def :datomic.client.transact/db-before :datomic.client/database)
(s/def :datomic.client.transact/db-after :datomic.client/database)
(s/def :datomic.client.transact/tempids (s/map-of string? :datomic/e))
(s/def :datomic.client.transact/tx-result
  (s/keys :req-un [:datomic.client.transact/db-before
                   :datomic.client.transact/db-after
                   :datomic.client.transact/tx-data
                   :datomic.client.transact/tempids]))
(s/def :datomic.client.transact/arg-map (s/keys :req-un [:datomic.client.transact/tx-data]))


(s/fdef datomic.client/transact
        :args (s/cat :conn :datomic.client/connection :arg-map :datomic.client.transact/arg-map)
        :ret channel?)

(s/fdef datomic.client/with-db
        :args (s/cat :conn :datomic.client/connection)
        :ret channel?)


(s/fdef datomic.client/with
        :args (s/cat :db :datomic.client/with-database :arg-map :datomic.client.transact/arg-map)
        :ret channel?)

(s/def :datomic.client.db-stats/datoms nat-int?)
(s/def :datomic.client.db-stats/result
  (s/keys :req-un [:datomic.client.db-stats/datoms]))

(s/fdef datomic.client/db-stats
        :args (s/cat :db :datomic.client/database)
        :ret channel?)

(s/fdef datomic.client.admin/list-databases
        :args (s/cat :arg-map map?)
        :ret channel?)

(s/fdef datomic.client.admin/create-database
        :args (s/cat :arg-map (s/keys :req-un [:datomic.client/db-name]))
        :ret channel?)

(s/fdef datomic.client.admin/delete-database
        :args (s/cat :arg-map (s/keys :req-un [:datomic.client/db-name]))
        :ret channel?)

(s/fdef datomic.client/error?
        :args (s/cat :response any?)
        :ret boolean?)


