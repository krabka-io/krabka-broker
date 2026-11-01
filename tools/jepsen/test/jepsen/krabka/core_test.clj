(ns jepsen.krabka.core-test
  "Unit tests of the harness's pure parts: option parsing, the suite runner's
  bookkeeping, the node configuration and the key mapping. They need no
  cluster.

    java -Djava.awt.headless=true -Dclojure.compile.path=target/jepsen/classes \\
      -cp tools/jepsen/src:tools/jepsen/test:$(cat target/jepsen/classpath.txt) \\
      clojure.main -m jepsen.krabka.core-test"
  (:require [clojure.test :refer [deftest is are testing run-tests]]
            [jepsen.krabka [client :as client]
                           [core :as core]
                           [db :as db]
                           [faketime :as faketime]]))

(def nodes ["n1" "n2" "n3" "n4" "n5"])

(deftest parse-faults-expands-special-names
  (are [spec faults] (= faults (core/parse-faults spec))
    "none"                 #{}
    "partition"            #{:partition}
    "partition,kill,pause" #{:partition :kill :pause}
    "standard"             #{:partition :kill :pause :packet}
    "all"                  #{:partition :kill :pause :packet :clock}
    "kill,standard"        #{:partition :kill :pause :packet}))

(deftest safe-then-overrides
  (let [{:keys [options errors]}
        (core/parse-test-args ["--safe" "--sub-via" "subscribe" "--txn"
                               "--bin-dir" "/bin" "--nodes" "n1,n2"])]
    (is (= nil errors))
    (is (= {:acks                             "all"
            :idempotence                      true
            :isolation-level                  "read_committed"
            :auto-offset-reset                "earliest"
            :enable-auto-commit               false
            :enable-server-auto-create-topics false
            :default-topic-replications       3
            :retries                          1000
            :sub-via                          #{:subscribe}
            :txn?                             true}
           (select-keys options [:acks :idempotence :isolation-level
                                 :auto-offset-reset :enable-auto-commit
                                 :enable-server-auto-create-topics
                                 :default-topic-replications :retries
                                 :sub-via :txn?])))))

(deftest refused-option-combinations
  (are [args error] (some #{error} (:errors (core/parse-test-args args)))
    ["--nodes" "n1"]
    "--bin-dir is required"

    ["--bin-dir" "/b" "--docker" "--clock-mode" "system" "--nemesis" "clock"]
    "--clock-mode system needs a clock per node; Docker containers share the host's"))

(deftest clock-mode-defaults-to-the-remote
  (are [args mode] (= mode (:clock-mode (:options (core/parse-test-args args))))
    ["--bin-dir" "/b" "--docker"]                           :faketime
    ["--bin-dir" "/b"]                                      :system
    ["--bin-dir" "/b" "--docker" "--clock-mode" "faketime"] :faketime
    ["--bin-dir" "/b" "--clock-mode" "faketime"]            :faketime))

(deftest split-suite-args-takes-out-the-suite-flags
  (is (= ["m.json" "r.json" ["--docker" "--nodes" "n1,n2"]]
         (core/split-suite-args ["--docker" "--manifest" "m.json" "--nodes"
                                 "n1,n2" "--results" "r.json"]))))

(deftest unmet-requirements
  (are [requires env why] (= why (core/unmet-requirement requires env))
    []                 {:netem? false} nil
    ["netem"]          {:netem? true}  nil
    ["netem"]          {:netem? false}
    "the nodes' kernel has no netem qdisc (packet faults)"
    ["warp-drive"]     {}              "unknown requirement warp-drive"))

(deftest test-names-are-directory-safe
  (are [opts name] (= name (core/test-name opts))
    {:sub-via #{:assign}}
    "krabka assign no-faults"

    {:sub-via #{:subscribe :assign} :txn? true :group-protocol "consumer"
     :crash-clients? true :acks "all" :idempotence true
     :isolation-level "read_committed" :nemesis #{:pause :kill}}
    (str "krabka assign,subscribe txn group=consumer crash-clients acks=all "
         "idem=true iso=read_committed kill,pause")))

(deftest keys-map-to-topic-partitions-and-back
  (are [k topic partition] (and (= topic (client/k->topic k))
                                (= partition (client/k->partition k))
                                (= k (client/topic-partition->k topic partition)))
    0  "t0"  0
    1  "t0"  1
    2  "t1"  0
    31 "t15" 1))

(deftest client-address-uses-recorded-ip
  (are [ips addr] (= addr (client/address {:node-ips (atom ips)} "n2"))
    {"n2" "172.18.0.3"} "172.18.0.3:9092"
    {}                  "n2:9092"))

(deftest node-ids-and-voters
  (let [test {:nodes nodes}]
    (is (= [1 3 5] (map (partial db/node-id test) ["n1" "n3" "n5"])))
    (is (= "n4" (db/node-by-id test 4)))
    (is (= ["1@n1:9093" "2@n2:9093" "3@n3:9093" "4@n4:9093" "5@n5:9093"]
           (db/quorum-voters test)))))

(deftest node-config
  (testing "the safe settings"
    (is (= (str "broker_id = 2\n"
                "log_dir = \"/opt/krabka/data\"\n"
                "controller_quorum_voters = [\"1@n1:9093\", \"2@n2:9093\", \"3@n3:9093\", \"4@n4:9093\", \"5@n5:9093\"]\n"
                "inter_broker_listener_name = \"PLAINTEXT\"\n"
                "\n"
                "[[listeners]]\n"
                "name = \"PLAINTEXT\"\n"
                "bind_addr = \"0.0.0.0:9092\"\n"
                "advertised = \"172.18.0.3:9092\"\n"
                "protocol = \"Plaintext\"\n"
                "\n"
                "[process]\n"
                "roles = [\"controller\", \"broker\"]\n"
                "\n"
                "[audit]\n"
                "enabled = false\n"
                "\n"
                "[runtime]\n"
                "classic_group_initial_rebalance_delay = \"0ms\"\n"
                "classic_group_min_session_timeout = \"1000ms\"\n"
                "default_min_insync_replicas = 2\n"
                "default_replication_factor = 3\n"
                "offsets_topic_replication_factor = 3\n"
                "transaction_state_min_isr = 2\n"
                "transaction_state_replication_factor = 3\n"
                "\n"
                "[server_properties]\n"
                "\"auto.create.topics.enable\" = \"false\"\n"
                "\"unclean.leader.election.enable\" = \"false\"\n")
           (db/config-toml {:nodes                            nodes
                            :default-topic-replications       3
                            :enable-server-auto-create-topics false}
                           "n2" "172.18.0.3"))))
  (testing "broker defaults stay unset"
    (is (= {"unclean.leader.election.enable" "false"}
           (db/server-properties {})))
    (is (not (contains? (db/runtime-settings {}) "default_replication_factor")))))

(deftest format-command
  (is (= ["/opt/krabka/bin/krabka-format" :--ignore-formatted
          :--log-dir "/opt/krabka/data"
          :--cluster-id "J3psenKrabkaClusterIdA"
          :--node-id 3]
         (db/format-command {:nodes nodes} "n3"))))

(deftest result-summaries
  (are [results status bad detail]
       (= {"name" "x", "status" status, "bad_error_types" bad,
           "error_types" ["G0" "duplicate"], "detail" detail, "store" "/s"}
          (with-redefs [jepsen.store/path (constantly "/s")]
            (core/result-summary
              "x" {:results (assoc-in results [:workload :error-types]
                                      [:G0 :duplicate])})))
    {:valid? true,  :workload {:valid? true}, :stats {:valid? true}}
    "valid" [] ""

    {:valid? false, :workload {:valid? false, :bad-error-types [:duplicate]},
     :stats {:valid? true}}
    "invalid" ["duplicate"] "invalid checkers: workload"

    {:valid? :unknown, :workload {:valid? :unknown}, :perf {:valid? true}}
    "unknown" [] "invalid checkers: workload"))

(deftest faketime-offsets-round-trip
  (are [ms text] (and (= text (faketime/offset-string ms))
                      (= ms (faketime/parse-offset text)))
    0       "+0.000"
    250     "+0.250"
    -12000  "-12.000"
    262144  "+262.144"))

(deftest faketime-reads-an-empty-file-as-no-offset
  (is (= 0 (faketime/parse-offset "")))
  (is (= -1500 (faketime/parse-offset " -1.500\n"))))

(deftest faketime-only-with-clock-faults
  (are [test on?] (= on? (db/faketime? test))
    {:clock-mode :faketime :faults #{:clock :kill}} true
    {:clock-mode :faketime :faults #{:kill}}        false
    {:clock-mode :system   :faults #{:clock}}       false))

(deftest test-map-keeps-the-faults
  (let [opts (:options (core/parse-test-args
                         ["--bin-dir" "/b" "--docker" "--nodes" "n1,n2,n3"
                          "--nemesis" "clock,kill"]))
        test (core/krabka-test opts)]
    (is (= #{:clock :kill} (:faults test)))
    (is (db/faketime? test))))

(defn -main
  [& _]
  (let [{:keys [fail error]} (run-tests 'jepsen.krabka.core-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))
