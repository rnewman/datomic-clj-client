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

(ns ^:skip-wiki datomic.client.conn-impl
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.core.async :as async :refer [take! chan <!! put! go <! >! >!! go-loop promise-chan timeout]]
            [cognitect.transit :as transit]
            [cognitect.http-client :as http]
            [cognitect.hmac-authn :as authn]
            [datomic.client.impl.types :as types])
  (:import [com.cognitect.transit ReadHandler]
           [datomic.client.impl BytesOutputStream ByteBufferInputStream]
           java.io.IOException
           [java.nio ByteBuffer]
           [java.security KeyStore]
           [java.security.cert X509Certificate CertificateFactory]))

(set! *warn-on-reflection* true)

(defn result-chan
  "Returns a chan with a result already on it."
  [result]
  (doto (promise-chan)
    (>!! result)))

(defn error-result
  "If x is an error, return a promise channel with it, else nil."
  [x]
  (when (:cognitect.anomalies/category x)
    (result-chan x)))

(defn ^ByteBufferInputStream bbuf->is [bbuf]
  (ByteBufferInputStream. bbuf))

(def read-handlers
  {"datom" (reify ReadHandler
             (fromRep [_ v] (apply types/datom v)))})

(def write-handlers
  {})

(defn marshal
  "Encodes val as Transit data in msgpack. Returns a map with keys:

  :bytes - a Java byte array of the marshalled data
  :length - the length in bytes of the marshalled data.

  Note that for efficiency's sake the byte array may be longer than
  the returned length."
  [m]
  (let [stm (BytesOutputStream.)
        w   (transit/writer stm :msgpack {:handlers write-handlers})]
    (transit/write w m)
    {:bytes  (.internalBuffer stm)
     :length (.length stm)}))

(defn unmarshal
  "Given a byte array containing a Transit value encoded using the specified
   decoder type and return the unmarshaled value. Defaults to :msgpack."
  ([stm]
    (unmarshal stm :msgpack))
  ([stm type]
   (let [r   (transit/reader stm type {:handlers read-handlers})
         res (transit/read r)]
     res)))

(defn norm-headers
  "Given request headers map, return a new request headers map with all
   headers keys lowercased."
  [headers]
  (reduce-kv
    (fn [acc ^String k v]
      (assoc acc (.toLowerCase k) v))
    {} headers))

(defn unmarshal-response
  "Given a Ring response unmarshal the ByteBuffer :body of the Ring response
using the decoding as specified by the :content-type of the response.
Returns a response map with updated :body."
  [{:keys [headers body] :as http-resp}]
  (if body
    (let [{:strs [content-type]} (norm-headers headers)
          ;;_ (prn ::unmarshal-body headers)
          stm (bbuf->is body)
          res (condp = content-type
                "application/transit+msgpack"
                (unmarshal stm :msgpack)

                "application/transit+json"
                (unmarshal stm :json)

                "application/edn"
                (edn/read (java.io.PushbackReader. (io/reader stm)))

                "text/plain"
                (slurp stm)
                
                {:cognitect.anomalies/category :cognitect.anomalies/fault
                 :cognitect.anomalies/message (str "Cannot unmarshal content-type " content-type)})]
      (assoc http-resp :body res))
    http-resp))

(declare send-http-request)

(defn advance-t
  [state new-state]
  (if (> (:t new-state) (:t state))
    new-state
    state))

(defn update-db-state [{:keys [db-id state] :as conn-impl} {:keys [dbs] :as resp}]
  (when-let [{:keys [t next-t]} (first dbs)]
    (when (and t next-t)
      (swap! state advance-t {:t t :next-t next-t}))))

(defn throwable->anomaly
  [^Throwable t]
  {:cognitect.anomalies/category :cognitect.anomalies/fault
   :cognitect.anomalies/message (.getMessage t)})

(defn- http-status->anom-cat
  "Return the anomaly kw for an http status."
  [status]
  (cond
   (= status 403) :cognitect.anomalies/forbidden
   (= status 503) :cognitect.anomalies/busy
   (= status 504) :cognitect.anomalies/unvailable
   (<= 400 status 499) :cognitect.anomalies/incorrect
   (<= 500 status 599) :cognitect.anomalies/fault))

(defn- http-client-kw->anom-cat
  "Return the anomaly kw for a :cognitect.http-client error"
  [code]
  (case code
        ::http/timeout :cognitect.anomalies/interrupted
        ::http/throttled :cognitect.anomalies/busy
        ::http/connect-failed :cognitect.anomalies/unavailable
        ::http/resolve-failed :cognitect.anomalies/not-found
        :cognitect.anomalies/fault))

(defn- http-status-anomaly
  "Returns an error based on http status if possible, or nil. This should only be
attempted after checking for better error info in the body."
  [{:keys [status body]}]
  (when-let [cat (http-status->anom-cat status)]
    {:cognitect.anomalies/category cat
     :datomic.client/http-error body}))

(defn- http-client-anomaly
  "Returns an anomaly based on cognitect.http-client ::http/error codes in the body,
else nil."
  [http-res]
  (when-let [http-client-kw (::http/error http-res)]
    (merge {:cognitect.anomalies/category (http-client-kw->anom-cat http-client-kw)}
           (when-let [^Throwable t (::http/throwable http-res)]
             {:cognitect.anomalies/message (str (.getName (.getClass t)) ": " (.getMessage t))}))))

(defn- http-body-anomaly
  "If http body represents an anomaly, return it, else nil."
  [http-res]
  (if (:cognitect.anomalies/category (:body http-res))
    (:body http-res)
    (http-client-anomaly http-res)))

(defn on-completed
  [conn-impl client-req http-res]
  (let [{:keys [status body] :as http-res} (unmarshal-response http-res)]
    (or (http-body-anomaly http-res)
        (http-status-anomaly http-res)
        (do
          (update-db-state conn-impl body)
          body))))

(defn catalog-op?
  [op]
  (= "datomic.catalog" (namespace op)))

(defn qualified-op [op]
  (if (catalog-op? op)
    (format "%s/%s" (namespace op) (name op))
    (str "datomic.client.protocol/" (name op))))

(defn client-req->http-req
  "Given a conn-impl and a Datomic client request return a Ring request where the
   :body is a ByteBuffer."
  [{:keys [scheme host port database-id] :as conn-impl}
   {:keys [length bytes op next-token]
    :or   {length 0
           bytes  (.getBytes "")}
    :as   client-req}]
  ;;(prn ::client-req->http-reg account-id db-name)
  (let [content-type "application/transit+msgpack"
        qualified-op (qualified-op op)]
    {:headers (merge {"host" host
                      "content-type" content-type
                      "accept" content-type
                      "x-nano-op" qualified-op}
                     (when-not (catalog-op? op) {"x-nano-target" database-id})
                     (when next-token {"x-nano-next" (str next-token)}))
     :scheme scheme
     :server-name host
     :server-port port
     :request-method :post
     :uri "/"
     :op qualified-op
     :body (ByteBuffer/wrap ^bytes bytes 0 length)
     :content-length length
     :content-type content-type}))

(defn sign-http-req
  "Given a conn-impl and a Ring compliant request, return a signed request as
   specified by the AWS Signature 4 signing algorithm."
  [{:keys [access-key service region secret] :as cfg} http-req]
  ;;(prn ::sign-http-req cfg http-req)
  (let [auth-info {:access-key access-key
                   :secret secret
                   :service service
                   :region region} 
        signed (authn/sign http-req auth-info)
;;        ch (chan 1)
;;        _ (authn/verify signed auth-info ch)
;;        verified (<!! ch)
        ]
    ;;(prn ::sign-http-req#raw http-req)
    ;;(prn ::sign-http-req#signed signed)
    ;;(prn ::sign-http-req#verified verified)
    signed))

(defn send-request
  "Given a conn-impl and an SPI request send the corresponding HTTP
   request."
  [{:keys [http-client] :as conn-impl}
   {:keys [length bytes]
    :or   {length 0
           bytes  (.getBytes "")}
    :as   client-req}
   timeout]
  (let [timeout (or timeout (:timeout conn-impl))
        retc (chan 1)
        req    (client-req->http-req conn-impl client-req)
        signed (sign-http-req conn-impl req)
        c      (chan)
        post   (cond-> signed
                       timeout (assoc ::http/timeout-msec timeout))]
    (http/submit http-client post c)
    (go
     (let [resp (<! c)]
       (put! retc (on-completed conn-impl client-req resp))))
    retc))

;; :trust-store-path "datomic/transactor-trust.jks"
;; :trust-store-password "transactor"

(defn trust-store
  []
  (let [trust-store (KeyStore/getInstance (KeyStore/getDefaultType))
        cacerts-filename (-> (System/getProperty "java.home")
                             (str "/lib/security/cacerts")
                             (string/replace "/" java.io.File/separator))
        cacerts-file (io/make-input-stream cacerts-filename {})
        cacerts-pwd (or (System/getProperty "datomic.client.cacertsPassword")
                        "changeit") ;; the Java-defined default
        pem-filename (io/resource "transactor-trust.pem")
        pem-file (io/make-input-stream pem-filename {})
        cert-factory (CertificateFactory/getInstance "X.509")
        pem-cert (.generateCertificate cert-factory pem-file)]
    (.load trust-store cacerts-file (.toCharArray cacerts-pwd))
    (.setCertificateEntry trust-store "datomic-client" pem-cert)
    trust-store))

(defonce http-client
  (delay
    (http/create
      {:trust-store (trust-store)})))

(def ^:private DEFAULT_HTTPS_PORT 443)

(defn- parse-endpoint
  [s]
  (when s
    (let [[_ host _ port] (re-find #"^([^:]+)(:(\d+)?)?$" s)
          port (if port (Long/parseLong port) DEFAULT_HTTPS_PORT)]
      (if (and host port)
        {:scheme "https" :host host :port port}
        (throw (IllegalArgumentException. (str "Not a valid endpoint " s)))))))

(defn create
  "Returns a new conn-impl object given a config map with the following
   required keys:

     :region - the AWS region
     :service - the service
     :account-id - the Datomic Cloud account id or constant for Pro peer-server
     :access-key - a Datomic Cloud account access-key value or constant for Pro peer-server
     :secret - a Datomic Cloud account secret value or secret configured when starting Pro peer-server
  "
  [config]
  (merge config
         (parse-endpoint (:endpoint config))
         {:http-client @http-client}))

(defn with-retry
  "Call req-fn, a function that wraps some operation and reutrns
a channel. Uses backoff to decide whether and how much to
backoff. When no more backoffs, puts result on result-chan."
  [req-fn result-chan backoff]
  (go-loop [msec nil]
    (when msec (<! (timeout msec)))
    (let [result (<! (req-fn))]
      (if-let [msec (backoff result)]
        (recur msec)
        (>! result-chan result))))
  result-chan)

(defn create-backoff
  "Returns a backoff function suitable for with-retry. Initial backoff of
start msecs, increasing by multiplicative factor so long as <= max."
  [pred start max factor]
  (let [a (atom (/ start factor))]
    #(when (pred %)
       (let [backoff (long (swap! a * factor))]
         (when (<= backoff max)
           backoff)))))

(defn queue-request
  "Accepts a Datomic request map and enqueues it on the HTTP conn-impl, returning
   a channel which delivers the response."
  [conn-impl req]
  (let [timeout (:timeout req)
        marshaled (marshal (dissoc req :timeout))
        marshaled (merge marshaled (select-keys req [:op :next-token]))
        result-ch (promise-chan)]
    (with-retry
      #(send-request conn-impl marshaled timeout)
      result-ch
      (create-backoff #(= :cognitect.anomalies/busy (:cognitect.anomalies/category %))
                      100 200 2))))
