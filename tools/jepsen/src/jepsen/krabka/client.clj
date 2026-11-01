(ns jepsen.krabka.client
  "The Jepsen client for `jepsen.tests.kafka`, built on Apache Kafka's Java
  clients.

  Each Jepsen worker holds one producer, one consumer and one admin client,
  all bound to one node. The workload names partitions by integer *keys*; key
  `k` is partition `k mod 2` of topic `t<k quot 2>`. Operations are:

    {:f :assign    :value [k ...]}  assign the consumer these partitions
    {:f :subscribe :value [k ...]}  subscribe the consumer to their topics
    {:f :send      :value [[:send k v] ...]}
    {:f :poll      :value [[:poll] ...]}
    {:f :txn       :value [[:send k v] [:poll] ...]}
    {:f :crash}                     tear the worker's clients down

  A `send` completes as `[:send k [offset v]]` and a `poll` as
  `[:poll {k [[offset v] ...]}]`. With `:txn?` every send, poll and txn op runs
  in a Kafka transaction that also commits the consumer's offsets.

  The structure follows the client of Jepsen's Redpanda and Kafka analyses
  (https://github.com/jepsen-io/redpanda), which `jepsen.tests.kafka` was
  extracted from, so that krabka's results read the same way theirs do."
  (:require [clojure.core.protocols :refer [Datafiable datafy]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [assert+ loopr]]
            [jepsen [client :as client]
                    [util :as util :refer [map-vals pprint-str]]]
            [clj-commons.slingshot :refer [try+ throw+]])
  (:import (java.time Duration)
           (java.util Properties)
           (java.util.concurrent ExecutionException TimeUnit)
           (org.apache.kafka.clients.admin Admin
                                           AdminClientConfig
                                           NewTopic
                                           TopicDescription)
           (org.apache.kafka.clients.consumer ConsumerConfig
                                              ConsumerRebalanceListener
                                              ConsumerRecord
                                              KafkaConsumer
                                              OffsetAndMetadata)
           (org.apache.kafka.clients.producer KafkaProducer
                                              ProducerConfig
                                              ProducerRecord
                                              RecordMetadata)
           (org.apache.kafka.common KafkaException
                                    KafkaFuture
                                    Node
                                    TopicPartition
                                    TopicPartitionInfo)
           (org.apache.kafka.common.errors AuthorizationException
                                           DisconnectException
                                           InterruptException
                                           InvalidProducerEpochException
                                           InvalidReplicationFactorException
                                           InvalidTopicException
                                           InvalidTxnStateException
                                           NetworkException
                                           NotControllerException
                                           NotLeaderOrFollowerException
                                           OutOfOrderSequenceException
                                           ProducerFencedException
                                           TimeoutException
                                           TopicExistsException
                                           TransactionAbortableException
                                           UnknownServerException
                                           UnknownTopicOrPartitionException
                                           WakeupException)))

;; Addresses

(def port
  "The broker listener of every node."
  9092)

(defn address
  "`host:port` of a node's broker listener. The DB records each node's IP when
  it starts the node, and every node advertises that IP, so the control node
  needs no name resolution for the DB nodes."
  [test node]
  (str (get (some-> (:node-ips test) deref) node node) ":" port))

;; Keys

(def partition-count
  "Partitions per topic."
  2)

(def replication-factor
  "Replicas per partition."
  3)

(defn k->topic [k] (str "t" (quot k partition-count)))

(defn k->partition [k] (mod k partition-count))

(defn ^TopicPartition k->topic-partition
  [k]
  (TopicPartition. (k->topic k) (int (k->partition k))))

(defn topic-partition->k
  ([^TopicPartition tp]
   (topic-partition->k (.topic tp) (.partition tp)))
  ([topic partition]
   (+ (* partition-count (parse-long (subs topic 1))) partition)))

(extend-protocol Datafiable
  TopicPartition
  (datafy [tp] {:topic (.topic tp), :partition (.partition tp)}))

;; Configuration

(def consumer-group
  "Every consumer is in one group."
  "jepsen-group")

(defn- ->properties
  ^Properties [m]
  (doto (Properties.) (.putAll (map-vals str m))))

(defn consumer-config
  "The consumer's configuration, from the test's options."
  [test node]
  (cond-> {ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG     (address test node)
           ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG
           "org.apache.kafka.common.serialization.LongDeserializer"
           ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG
           "org.apache.kafka.common.serialization.LongDeserializer"
           ; sendOffsetsToTransaction needs a group even for assigned
           ; partitions.
           ConsumerConfig/GROUP_ID_CONFIG                   consumer-group
           ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG     500
           ConsumerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000
           ConsumerConfig/METADATA_MAX_AGE_CONFIG          60000
           ConsumerConfig/REQUEST_TIMEOUT_MS_CONFIG        10000
           ConsumerConfig/DEFAULT_API_TIMEOUT_MS_CONFIG    10000
           ConsumerConfig/CONNECTIONS_MAX_IDLE_MS_CONFIG   60000}

    ; The classic protocol takes its session timing from the client; KIP-848's
    ; consumer protocol takes it from the group coordinator and rejects these.
    (not= "consumer" (:group-protocol test))
    (assoc ConsumerConfig/HEARTBEAT_INTERVAL_MS_CONFIG 300
           ConsumerConfig/SESSION_TIMEOUT_MS_CONFIG    6000)

    (:group-protocol test)
    (assoc ConsumerConfig/GROUP_PROTOCOL_CONFIG (:group-protocol test))

    (some? (:isolation-level test))
    (assoc ConsumerConfig/ISOLATION_LEVEL_CONFIG (:isolation-level test))

    (some? (:auto-offset-reset test))
    (assoc ConsumerConfig/AUTO_OFFSET_RESET_CONFIG (:auto-offset-reset test))

    (some? (:enable-auto-commit test))
    (assoc ConsumerConfig/ENABLE_AUTO_COMMIT_CONFIG (:enable-auto-commit test))))

(defn producer-config
  "The producer's configuration, from the test's options. Timeouts are short so
  that a fault shows up as an indefinite result rather than a stalled worker."
  [test node transactional-id]
  (cond-> {ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (address test node)
           ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG
           "org.apache.kafka.common.serialization.LongSerializer"
           ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG
           "org.apache.kafka.common.serialization.LongSerializer"
           ProducerConfig/CLIENT_ID_CONFIG             (str "jepsen-" transactional-id)
           ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG   10000
           ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG    3000
           ProducerConfig/MAX_BLOCK_MS_CONFIG          10000
           ProducerConfig/TRANSACTION_TIMEOUT_CONFIG   1000
           ProducerConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG 1000
           ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG     500
           ProducerConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000}

    (some? (:acks test))
    (assoc ProducerConfig/ACKS_CONFIG (:acks test))

    (some? (:idempotence test))
    (assoc ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG (:idempotence test))

    (some? (:retries test))
    (assoc ProducerConfig/RETRIES_CONFIG (:retries test))

    (:txn? test)
    (assoc ProducerConfig/TRANSACTIONAL_ID_CONFIG transactional-id)))

(defn admin-config
  [test node]
  {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG       (address test node)
   AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG  3000
   AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG      3000
   AdminClientConfig/RECONNECT_BACKOFF_MAX_MS_CONFIG 1000
   AdminClientConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG     500
   AdminClientConfig/SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG 1000
   AdminClientConfig/RETRIES_CONFIG                 0})

;; Client construction

(def ^:private transactional-ids (atom -1))

(def ^:private configs-logged? (atom false))

(defn ^Admin open-admin
  [test node]
  (Admin/create (->properties (admin-config test node))))

(defn- ms ^Duration [n] (Duration/ofMillis n))

(defn close-admin!
  "Closes an admin client at once. Its plain close waits for every call in
  flight, which a partition can hold for minutes."
  [^Admin a]
  (.close a (Duration/ofMillis 0)))

(defn- close-producer!
  [^KafkaProducer p]
  (.close p (ms 0)))

(defn- close-consumer!
  "Closes a consumer. A consumer can hang in close, even with a zero timeout,
  so a second thread wakes it after a second."
  [^KafkaConsumer c]
  (let [waker (future (Thread/sleep 1000) (.wakeup c))]
    (try (.close c (ms 0))
         (catch WakeupException _ nil)
         (finally (future-cancel waker)))))

(defn- open-producer
  "A producer bound to a node. A transactional producer that fails
  initTransactions is unusable, so each attempt starts a new one."
  [test node transactional-id]
  (let [config (->properties (producer-config test node transactional-id))]
    (if-not (:txn? test)
      (KafkaProducer. config)
      (util/await-fn
        (fn init []
          (let [p (KafkaProducer. config)]
            (try (.initTransactions p)
                 p
                 (catch Throwable t
                   (close-producer! p)
                   (throw t)))))
        {:log-interval 5000
         :log-message  "Waiting for initTransactions()"}))))

(defn- open-consumer
  [test node]
  (KafkaConsumer. (->properties (consumer-config test node))))

(defn create-topic!
  "Creates the topic of a key, treating one that already exists as created."
  [^Admin admin topic]
  (try
    (let [^KafkaFuture created (-> admin
                                   (.createTopics [(NewTopic. ^String topic
                                                              (int partition-count)
                                                              (short replication-factor))])
                                   .values
                                   (.get topic))]
      (.get created 10 TimeUnit/SECONDS))
    (catch ExecutionException e
      (if (instance? TopicExistsException (.getCause e))
        :exists
        (throw e)))))

(defmacro unwrap
  "Rethrows the KafkaException inside an ExecutionException."
  [& body]
  `(try ~@body
        (catch ExecutionException e#
          (let [cause# (util/ex-root-cause e#)]
            (throw (if (instance? KafkaException cause#) cause# e#))))))

;; Micro-operations

(def poll-ms
  "How long one poll waits."
  100)

(defn- poll!
  [{:keys [^KafkaConsumer consumer]} duration-ms]
  (try
    (let [records (.poll consumer (ms duration-ms))]
      [:poll (into (sorted-map)
                   (for [^TopicPartition tp (.partitions records)]
                     [(topic-partition->k tp)
                      (mapv (fn [^ConsumerRecord r] [(.offset r) (.value r)])
                            (.records records tp))]))])
    (catch InvalidTopicException _ [:poll {}])
    (catch IllegalStateException e
      ; A consumer with no assignment and no subscription reads nothing.
      (if (re-find #"not subscribed to any" (str (.getMessage e)))
        [:poll {}]
        (throw e)))))

(defn- send!
  [{:keys [admin ^KafkaProducer producer extant-topics]} [f k v]]
  (let [topic (k->topic k)]
    (when-not (contains? @extant-topics topic)
      (create-topic! admin topic)
      (swap! extant-topics conj topic))
    (let [record (ProducerRecord. topic (int (k->partition k)) nil v)
          ^RecordMetadata res (or (deref (.send producer record) 10000 nil)
                                  (throw+ {:type :timeout}))
          k' (topic-partition->k (.topic res) (.partition res))]
      (assert+ (= k k') {:type ::key-mismatch, :k k, :k' k'})
      [f k [(when (.hasOffset res) (.offset res)) v]])))

(defn mop!
  "Applies one micro-operation and returns its completed form."
  [client mop poll-duration]
  (case (first mop)
    :poll (poll! client poll-duration)
    :send (send! client mop)))

;; Transactions and rollback

(defn- polled
  "The poll results in an op's value, as [k [[offset v] ...]] pairs."
  [value]
  (for [[f m] value :when (= :poll f), pair m] pair))

(defn- rollback-consumer!
  "Seeks the consumer back to the first offset this op polled from each key,
  so that a failed or indefinite op's reads are read again."
  [^KafkaConsumer consumer value]
  (let [firsts (reduce (fn [m [k pairs]]
                         (if (or (contains? m k) (empty? pairs))
                           m
                           (assoc m k (ffirst pairs))))
                       {}
                       (polled value))]
    (doseq [[k offset] firsts]
      (try (.seek consumer (k->topic-partition k) (long offset))
           ; Rebalanced away from us; nothing to roll back.
           (catch IllegalStateException _ nil)))))

(defn- send-offsets!
  "Adds the next offset after each key's highest read to the transaction."
  [{:keys [^KafkaProducer producer ^KafkaConsumer consumer]} value]
  (let [offsets (reduce (fn [m [k pairs]]
                          (reduce (fn [m [offset _]]
                                    (update m k (fnil max offset) offset))
                                  m pairs))
                        {}
                        (polled value))]
    (when (seq offsets)
      (.sendOffsetsToTransaction
        producer
        (into {} (for [[k offset] offsets]
                   [(k->topic-partition k) (OffsetAndMetadata. (inc offset))]))
        (.groupMetadata consumer)))))

(defn- abort!
  "Aborts the open transaction after `cause`, then throws {:type :abort}.
  `definite?` says whether the transaction certainly did not commit."
  [{:keys [^KafkaProducer producer]} tried-commit? cause]
  (try (.abortTransaction producer)
       (catch RuntimeException abort-error
         (throw+ {:type          :abort
                  :abort-ok?     false
                  :tried-commit? tried-commit?
                  :definite?     (or (not tried-commit?)
                                     (instance? InvalidTxnStateException cause))
                  :body-error    cause
                  :abort-error   abort-error})))
  (throw+ {:type          :abort
           :abort-ok?     true
           :tried-commit? tried-commit?
           :definite?     true
           :body-error    cause}))

(defn- maybe-abort!
  "Throws inside a transaction with probability :abort-p, to exercise aborts."
  [test]
  (when (and (:txn? test) (< (rand) (:abort-p test 0)))
    (throw+ {:type :intentional-abort})))

(defmacro ^:private with-txn
  "Runs body in a Kafka transaction when the test has :txn?. Body returns the
  op's value; its reads' offsets go into the transaction before commit.
  Fencing, sequence and authorization errors cannot be aborted and propagate.
  So do timeouts and interrupts during commit, which are indefinite."
  [test client value-atom & body]
  `(if-not (:txn? ~test)
     (do ~@body)
     (let [producer# ^KafkaProducer (:producer ~client)]
       (.beginTransaction producer#)
       (let [result# (try (let [r# (do ~@body)]
                            (send-offsets! ~client @~value-atom)
                            r#)
                          (catch ProducerFencedException e# (throw e#))
                          (catch OutOfOrderSequenceException e# (throw e#))
                          (catch AuthorizationException e# (throw e#))
                          (catch RuntimeException e#
                            (abort! ~client false e#)))]
         (try (.commitTransaction producer#)
              (catch ProducerFencedException e# (throw e#))
              (catch OutOfOrderSequenceException e# (throw e#))
              (catch AuthorizationException e# (throw e#))
              (catch TimeoutException e# (throw e#))
              (catch InterruptException e# (throw e#))
              (catch RuntimeException e#
                (abort! ~client true e#)))
         result#))))

(defn- error-string [e]
  (if (instance? clojure.lang.ExceptionInfo e) (ex-data e) (str e)))

(defmacro ^:private with-errors
  "Turns the exceptions of an op into :fail (definitely did not happen) or
  :info (may have happened). :end-process? retires the worker's clients."
  [op & body]
  `(try+
     ~@body
     (catch AuthorizationException _#
       (assoc ~op :type :fail, :error :authorization, :end-process? true))
     (catch DisconnectException e#
       (assoc ~op :type :info, :error [:disconnect (.getMessage e#)]))
     (catch InvalidProducerEpochException e#
       (assoc ~op :type :fail, :error [:invalid-producer-epoch (.getMessage e#)]))
     (catch InvalidTopicException _#
       (assoc ~op :type :fail, :error :invalid-topic))
     (catch InvalidReplicationFactorException _#
       (assoc ~op :type :fail, :error :invalid-replication-factor))
     (catch NetworkException e#
       (assoc ~op :type :info, :error [:network (.getMessage e#)]))
     (catch NotControllerException _#
       (assoc ~op :type :fail, :error :not-controller))
     ; Not definite: see KAFKA-13574.
     (catch NotLeaderOrFollowerException _#
       (assoc ~op :type :info, :error :not-leader-or-follower))
     (catch OutOfOrderSequenceException _#
       (assoc ~op :type :fail, :error :out-of-order-sequence, :end-process? true))
     (catch ProducerFencedException _#
       (assoc ~op :type :fail, :error :producer-fenced, :end-process? true))
     (catch TransactionAbortableException e#
       (assoc ~op :type :fail, :error [:transaction-abortable (.getMessage e#)]
              :end-process? true))
     (catch UnknownTopicOrPartitionException _#
       (assoc ~op :type :fail, :error :unknown-topic-or-partition))
     (catch UnknownServerException e#
       (assoc ~op :type :info, :error [:unknown-server-exception (.getMessage e#)]))
     (catch TimeoutException _#
       (assoc ~op :type :info, :error :kafka-timeout))
     (catch IllegalStateException e#
       (if (re-find #"Invalid transition attempted" (str (.getMessage e#)))
         (assoc ~op :type :info, :error [:illegal-transition (.getMessage e#)]
                :end-process? true)
         (throw e#)))
     (catch KafkaException e#
       (let [msg# (str (.getMessage e#))]
         (condp re-find msg#
           #"broker is not available"
           (assoc ~op :type :fail, :error :broker-not-available)

           #"Cannot execute transactional method because we are in an error state"
           (assoc ~op :type :fail, :error [:txn-in-error-state msg#], :end-process? true)

           #"Topic or Partition .+? does not exist"
           (assoc ~op :type :fail, :error [:topic-partition-does-not-exist msg#])

           #"Unexpected error in AddOffsetsToTxnResponse"
           (assoc ~op :type :fail, :error [:add-offsets msg#])

           #"Unexpected error in TxnOffsetCommitResponse"
           (assoc ~op :type :fail, :error [:txn-offset-commit msg#])

           #"Unhandled error in EndTxnResponse"
           (assoc ~op :type :info, :error [:end-txn msg#])

           (throw e#))))
     (catch [:type :abort] e#
       (assoc ~op
              :type         (if (:definite? e#) :fail :info)
              :end-process? (not (:abort-ok? e#))
              :error        (cond-> (update e# :body-error error-string)
                              (:abort-error e#) (update :abort-error error-string))))
     (catch [:type :partitions-assigned] e# (assoc ~op :type :fail, :error e#))
     (catch [:type :partitions-revoked] e# (assoc ~op :type :fail, :error e#))
     (catch [:type :partitions-lost] e# (assoc ~op :type :fail, :error e#))
     (catch [:type :timeout] _#
       (assoc ~op :type :info, :error :timeout))))

(defn- rebalance-listener
  "Journals rebalance events into an atom of a vector."
  [log]
  (letfn [(note [kind tps]
            (swap! log conj {:type kind
                             :keys (mapv topic-partition->k tps)}))]
    (reify ConsumerRebalanceListener
      (onPartitionsRevoked [_ tps] (note :revoked tps))
      (onPartitionsAssigned [_ tps] (note :assigned tps))
      (onPartitionsLost [_ tps] (note :lost tps)))))

(defn- txn!
  "Runs a :send, :poll or :txn op. The op's value lives in an atom while it
  runs, so that offsets learned before a failure stay in the completed op."
  [{:keys [^KafkaConsumer consumer rebalance-log] :as client} test op]
  (let [value (atom (:value op))
        sleep (if (< (rand) 0.5) 0 (rand-int (inc (:intra-txn-delay test 0))))
        _     (reset! rebalance-log [])
        op'   (try
                (with-errors op
                  (with-txn test client value
                    (unwrap
                      (doseq [i (range (count @value))]
                        (maybe-abort! test)
                        (swap! value assoc i
                               (mop! client (nth @value i) (:poll-ms op poll-ms)))
                        (when (pos? sleep) (Thread/sleep (long sleep))))
                      (maybe-abort! test)
                      ; Outside a transaction a subscribed consumer commits its
                      ; position itself. A failed commit keeps the reads: the
                      ; broker may have advanced the group regardless.
                      (if (and (#{:poll :txn} (:f op))
                               (not (:txn? test))
                               (:subscribe (:sub-via test)))
                        (try (.commitSync consumer)
                             (assoc op :type :ok)
                             (catch RuntimeException e
                               (assoc op :type :ok
                                      :error [:consumer-commit (.getMessage e)])))
                        (assoc op :type :ok)))))
                (catch Throwable t
                  (rollback-consumer! consumer @value)
                  (throw t)))
        op'   (assoc op' :value @value)]
    (when-not (= :ok (:type op'))
      (rollback-consumer! consumer @value))
    (if (seq @rebalance-log)
      (assoc op' :rebalance-log @rebalance-log)
      op')))

(defn partition-states
  "Each key's partition as `DescribeTopics` reports it: leader, ISR and
  replicas by node id. The workload asks for these in its final phase, to
  explain a key whose reads never caught up."
  [^Admin admin ks]
  (let [topics (distinct (map k->topic ks))
        descs  (-> admin
                   (.describeTopics ^java.util.Collection topics)
                   .allTopicNames
                   (.get 5 TimeUnit/SECONDS))]
    (into (sorted-map)
          (for [k ks
                :let [^TopicDescription d (.get ^java.util.Map descs (k->topic k))
                      ^TopicPartitionInfo p (when d (nth (.partitions d) (k->partition k) nil))]
                :when p]
            [k {:leader   (some-> (.leader p) .id)
                :isr      (mapv #(.id ^Node %) (.isr p))
                :replicas (mapv #(.id ^Node %) (.replicas p))}]))))

(defrecord Client [node admin producer consumer extant-topics rebalance-log]
  client/Client
  (open! [this test node]
    (let [tx-id (str "jt" (swap! transactional-ids inc))]
      (when (compare-and-set! configs-logged? false true)
        (info "Producer config:\n" (pprint-str (producer-config test node tx-id)))
        (info "Consumer config:\n" (pprint-str (consumer-config test node))))
      (assoc this
             :node          node
             :admin         (open-admin test node)
             :producer      (open-producer test node tx-id)
             :consumer      (open-consumer test node)
             :extant-topics (atom #{})
             :rebalance-log (atom []))))

  (setup! [_ _test])

  (invoke! [this test op]
    (case (:f op)
      :assign
      (let [tps (mapv k->topic-partition (:value op))]
        (.assign ^KafkaConsumer consumer tps)
        (when (:seek-to-beginning? op)
          (.seekToBeginning ^KafkaConsumer consumer tps))
        (assoc op :type :ok))

      :subscribe
      (let [topics (distinct (map k->topic (:value op)))]
        (.subscribe ^KafkaConsumer consumer
                    ^java.util.Collection topics
                    ^ConsumerRebalanceListener (rebalance-listener rebalance-log))
        (assoc op :type :ok))

      :crash
      (assoc op :type :info)

      :debug-topic-partitions
      (try
        (assoc op :type :ok, :value {:node node
                                     :partitions (partition-states admin (:value op))})
        (catch Exception e
          (assoc op :type :fail, :error (str (util/ex-root-cause e)))))

      (:poll :send :txn)
      (txn! this test op)))

  (teardown! [_ _test])

  (close! [_ _test]
    (close-admin! admin)
    (close-producer! producer)
    (close-consumer! consumer))

  client/Reusable
  ; A crashed op ends the process and its clients; Jepsen opens fresh ones.
  (reusable? [_ _test] false))

(defn client
  "A fresh workload client."
  []
  (map->Client {}))
