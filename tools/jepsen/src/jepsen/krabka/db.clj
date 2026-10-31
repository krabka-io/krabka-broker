(ns jepsen.krabka.db
  "Installs, configures, starts and faults a krabka cluster.

  Every node runs `krabka-broker` in KRaft combined mode: it is both a broker
  and a voter of the static controller quorum, as in Kafka's
  `process.roles=broker,controller`. The test uploads the two binaries named by
  `--bin-dir` to each node, so the nodes need no package of krabka."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.krabka.client :as kc]
            [clj-commons.slingshot :refer [try+ throw+]])
  (:import (java.util.concurrent TimeUnit)
           (org.apache.kafka.clients.admin Admin QuorumInfo)))

(def dir "Where a node keeps krabka." "/opt/krabka")
(def bin-dir (str dir "/bin"))
(def broker-bin (str bin-dir "/krabka-broker"))
(def format-bin (str bin-dir "/krabka-format"))
(def data-dir (str dir "/data"))
(def config-file (str dir "/krabka.toml"))
(def log-file (str dir "/broker.log"))
(def pid-file (str dir "/broker.pid"))
(def data-archive (str dir "/data.tar.bz2"))

(def binaries
  "The binaries a node runs, by file name."
  ["krabka-broker" "krabka-format"])

(def controller-port
  "The controller listener of every node."
  9093)

(def cluster-id
  "The cluster id every node is formatted with: a Kafka base64 UUID."
  "J3psenKrabkaClusterIdA")

(def started-line
  "What a node logs once its start is complete, whatever its roles."
  "krabka-broker started")

(defn node-id
  "A node's KRaft id: its position in the test's node list, from 1."
  [test node]
  (inc (.indexOf ^java.util.List (vec (:nodes test)) node)))

(defn node-by-id
  "The node with this KRaft id."
  [test id]
  (nth (:nodes test) (dec id) nil))

(defn quorum-voters
  "The static voter set: every node, at its controller listener."
  [test]
  (mapv (fn [node] (str (node-id test node) "@" node ":" controller-port))
        (:nodes test)))

(defn- toml-string
  [s]
  (str "\"" (str/escape (str s) {\" "\\\"", \\ "\\\\"}) "\""))

(defn- toml-value
  [v]
  (cond (string? v)     (toml-string v)
        (sequential? v) (str "[" (str/join ", " (map toml-value v)) "]")
        :else           (str v)))

(defn- toml-table
  "Lines of one TOML table, keys sorted and keys quoted where Kafka's dotted
  names need it."
  [header m]
  (concat [(str "[" header "]")]
          (for [[k v] (sort-by key m)]
            (str (if (re-find #"^[a-z_]+$" k) k (toml-string k))
                 " = " (toml-value v)))
          [""]))

(defn runtime-settings
  "The `[runtime]` table: the broker settings the test's options ask for, plus
  replication factors for the internal topics that match a five-node cluster.
  Group rebalancing starts at once and consumer sessions may be as short as
  Jepsen's clients ask for."
  [test]
  (cond-> {"default_min_insync_replicas"          2
           "offsets_topic_replication_factor"     3
           "transaction_state_replication_factor" 3
           "transaction_state_min_isr"            2
           "classic_group_initial_rebalance_delay" "0ms"
           "classic_group_min_session_timeout"     "1000ms"}
    (:default-topic-replications test)
    (assoc "default_replication_factor" (:default-topic-replications test))))

(defn server-properties
  "Kafka-named keys the broker reads from `[server_properties]`."
  [test]
  (cond-> {"unclean.leader.election.enable" "false"}
    (contains? test :enable-server-auto-create-topics)
    (assoc "auto.create.topics.enable"
           (str (boolean (:enable-server-auto-create-topics test))))))

(defn config-toml
  "The `krabka.toml` of one node. `advertised-host` is where clients and the
  other brokers reach this node's broker listener."
  [test node advertised-host]
  (str/join
    "\n"
    (concat
      [(str "broker_id = " (node-id test node))
       (str "log_dir = " (toml-string data-dir))
       (str "controller_quorum_voters = " (toml-value (quorum-voters test)))
       (str "inter_broker_listener_name = " (toml-string "PLAINTEXT"))
       ""
       "[[listeners]]"
       (str "name = " (toml-string "PLAINTEXT"))
       (str "bind_addr = " (toml-string (str "0.0.0.0:" kc/port)))
       (str "advertised = " (toml-string (str advertised-host ":" kc/port)))
       (str "protocol = " (toml-string "Plaintext"))
       ""
       "[process]"
       (str "roles = " (toml-value ["controller" "broker"]))
       ""
       ; The audit log is a krabka extension; its topic would take a replica
       ; and a partition the test does not account for.
       "[audit]"
       "enabled = false"
       ""]
      (toml-table "runtime" (runtime-settings test))
      (toml-table "server_properties" (server-properties test)))))

(defn format-command
  "The `krabka-format` invocation for a node."
  [test node]
  [format-bin
   :--ignore-formatted
   :--log-dir data-dir
   :--cluster-id cluster-id
   :--node-id (node-id test node)])

(defn install!
  "Uploads the test's binaries to this node."
  [test]
  (let [local-dir (or (:bin-dir test)
                      (throw+ {:type ::no-bin-dir
                               :message "--bin-dir names no directory"}))]
    (c/su
      (c/exec :mkdir :-p bin-dir data-dir)
      (doseq [b binaries]
        (c/upload (str local-dir "/" b) (str bin-dir "/" b))
        (c/exec :chmod :0755 (str bin-dir "/" b))))))

(defn configure!
  "Writes this node's configuration and formats its log directory."
  [test node]
  (c/su
    (cu/write-file! (config-toml test node (cn/local-ip)) config-file)
    (apply c/exec (concat (format-command test node)
                          [:>> log-file (c/lit "2>&1")]))))

(defn await-started!
  "Waits until the broker on this node logs that its start is complete. Fails
  early with the broker's last error line if it exits on the way."
  [timeout-ms]
  (util/await-fn
    (fn started? []
      (when-not (cu/daemon-running? pid-file)
        (throw+ {:type    ::exited
                 :message (str "krabka-broker exited during startup: "
                               (c/exec (c/lit (str "grep '^Error' " log-file
                                                   " | tail -1 || true"))))}))
      (c/exec :grep :-q started-line log-file)
      true)
    {:timeout      timeout-ms
     :retry-interval 500
     :log-interval 10000
     :log-message  "Waiting for krabka-broker to start"}))

(defn controller-leader
  "The node that the controller quorum reports as its leader, asked of `node`
  through Kafka's `DescribeMetadataQuorum`, or nil when it cannot tell."
  [test node]
  (let [admin (kc/open-admin test node)]
    (try
      (let [^QuorumInfo quorum (-> admin .describeMetadataQuorum .quorumInfo
                                   (.get 3 TimeUnit/SECONDS))]
        (node-by-id test (.leaderId quorum)))
      (catch Exception e
        (info "No controller leader from" node ":" (str e))
        nil)
      (finally (kc/close-admin! admin)))))

(defrecord DB []
  db/DB
  (setup! [this test node]
    ; Clients on the control node reach each node by the IP it advertises.
    (swap! (:node-ips test) assoc node (cn/local-ip))
    (install! test)
    (configure! test node)
    (db/start! this test node)
    (await-started! 120000)
    ; Every node must be up before the first client creates a topic.
    (jepsen/synchronize test))

  (teardown! [this test node]
    (db/kill! this test node)
    (c/su (c/exec :rm :-rf dir)))

  db/Process
  (start! [_ test node]
    (c/su
      (cu/start-daemon!
        {:chdir   dir
         :logfile log-file
         :pidfile pid-file}
        broker-bin
        :--config-file config-file
        :--controller-listen-addr (str "0.0.0.0:" controller-port)
        :--metrics-listen-addr "none"
        :--health-listen-addr "none")))

  (kill! [_ test node]
    (c/su
      (cu/stop-daemon! "krabka-broker" pid-file)
      (c/exec :rm :-f pid-file)))

  db/Pause
  (pause! [_ test node]
    (c/su (cu/grepkill! :stop "krabka-broker")))

  (resume! [_ test node]
    (c/su (cu/grepkill! :cont "krabka-broker")))

  db/Primary
  (setup-primary! [_ test node])

  (primaries [_ test]
    ; The controller leader is the one node whose loss makes the quorum elect.
    (->> (:nodes test)
         shuffle
         (some (partial controller-leader test))
         vector
         (remove nil?)))

  db/LogFiles
  (log-files [_ test node]
    (c/su
      (try+
        (c/exec :tar :cjf data-archive :-C dir "data")
        (catch [:type :jepsen.control/nonzero-exit] e
          (warn "Could not archive the data directory on" node ":" (:err e)))))
    {log-file     "broker.log"
     config-file  "krabka.toml"
     data-archive "data.tar.bz2"}))

(defn db
  "A krabka cluster."
  []
  (DB.))
