(ns jepsen.krabka.core
  "Runs Jepsen's Kafka workload, `jepsen.tests.kafka`, against krabka.

  Three commands:

    test   one test, with Jepsen's usual options plus the ones below
    suite  every entry of a manifest, one test each, with a JSON summary
    serve  Jepsen's web view of the store directory

  The workload, its generator and its checker are Jepsen's. This harness adds
  the client (`jepsen.krabka.client`), the DB (`jepsen.krabka.db`), Jepsen's
  combined nemesis, and the options of Jepsen's own Kafka and Redpanda tests."
  (:require [clojure [string :as str]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.tools.cli :as tools.cli]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [control :as c]
                    [core :as jepsen]
                    [generator :as gen]
                    [history :as h]
                    [os :as os]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.control.net :as cn]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os.debian :as debian]
            [jepsen.tests.kafka :as kafka]
            [jepsen.krabka [client :as client]
                           [db :as db]
                           [docker :as docker]
                           [faketime :as faketime]]))

(def jepsen-version
  "The Jepsen release this harness runs, as pom.xml pins it."
  "0.3.15")

;; Faults

(def faults
  "The faults the nemesis can inject."
  #{:partition :kill :pause :packet :clock})

(def special-faults
  "Names for sets of faults."
  {:none []
   ; Everything that leaves each node's clock its own.
   :standard [:partition :kill :pause :packet]
   :all      [:partition :kill :pause :packet :clock]})

(defn parse-comma-kws
  "`a,b` as `(:a :b)`."
  [spec]
  (->> (str/split spec #",") (remove str/blank?) (map keyword)))

(defn parse-faults
  "A comma-separated fault list, with the special names expanded."
  [spec]
  (->> (parse-comma-kws spec)
       (mapcat #(get special-faults % [%]))
       set))

(def db-targets #{:one :primaries :minority :majority :minority-third :all})

(def partition-targets
  #{:one :primaries :minority-third :majority :majorities-ring})

(defn nemesis-package
  "Jepsen's combined nemesis, built from the packages of the selected faults
  only. `nc/nemesis-package` sets up every package's nemesis whatever the
  faults, and the setup of its file-corruption and clock nemeses downloads and
  compiles tools on each node."
  [opts db]
  (let [faults     (set (:nemesis opts))
        clock-mode (:clock-mode opts)
        opts   {:db        db
                :nodes     (:nodes opts)
                :faults    faults
                :interval  (:nemesis-interval opts)
                :partition {:targets (:partition-targets opts)}
                :kill      {:targets (:db-targets opts)}
                :pause     {:targets (:db-targets opts)}
                :packet    {:targets   (:db-targets opts)
                            :behaviors [{:delay {}}
                                        {:loss {:percent :20%}}
                                        {:delay {} :duplicate {:percent :25%}}
                                        {:reorder {:percent :25%}}]}
                :clock     {:targets (:db-targets opts)}}]
    (nc/compose-packages
      (cond-> []
        (faults :partition)                   (conj (nc/partition-package opts))
        (faults :packet)                      (conj (nc/packet-package opts))
        (faults :clock)                       (conj (if (= :faketime clock-mode)
                                                      (faketime/package opts)
                                                      (nc/clock-package opts)))
        (some faults [:kill :pause])          (conj (nc/db-package opts))))))

;; Test

(def logging-overrides
  "The Kafka clients log every leader change and retry; keep that out of the
  test log."
  {"org.apache.kafka.clients.FetchSessionHandler"                     :warn
   "org.apache.kafka.clients.Metadata"                                :off
   "org.apache.kafka.clients.NetworkClient"                           :error
   "org.apache.kafka.clients.admin.AdminClientConfig"                 :warn
   "org.apache.kafka.clients.admin.KafkaAdminClient"                  :warn
   "org.apache.kafka.clients.admin.internals.AdminMetadataManager"    :warn
   "org.apache.kafka.clients.consumer.ConsumerConfig"                 :warn
   "org.apache.kafka.clients.consumer.internals"                      :warn
   "org.apache.kafka.clients.consumer.KafkaConsumer"                  :warn
   "org.apache.kafka.clients.producer.KafkaProducer"                  :warn
   "org.apache.kafka.clients.producer.ProducerConfig"                 :warn
   "org.apache.kafka.clients.producer.internals.Sender"               :off
   "org.apache.kafka.clients.producer.internals.TransactionManager"   :warn
   "org.apache.kafka.common.metrics.Metrics"                          :warn
   "org.apache.kafka.common.telemetry.internals.KafkaMetricsCollector" :warn
   "org.apache.kafka.common.utils.AppInfoParser"                      :warn})

(defn test-name
  "A name that says what the test ran, and that is safe as a directory name."
  [opts]
  (str/join " "
            (remove nil?
                    ["krabka"
                     (->> (:sub-via opts) (map name) sort (str/join ","))
                     (when (:txn? opts) "txn")
                     (when-let [p (:group-protocol opts)] (str "group=" p))
                     (when (:crash-clients? opts) "crash-clients")
                     (when-let [a (:acks opts)] (str "acks=" a))
                     (when (contains? opts :idempotence)
                       (str "idem=" (:idempotence opts)))
                     (when-let [i (:isolation-level opts)] (str "iso=" i))
                     (let [n (:nemesis opts)]
                       (if (seq n)
                         (->> n (map name) sort (str/join ","))
                         "no-faults"))])))

(defn stats-checker
  "Jepsen's stats, ignoring :crash ops, which never succeed by design."
  []
  (kafka/stats-checker))

(defn perf-checker
  "Latency and rate plots without the bookkeeping ops."
  [nemeses]
  (let [c (checker/perf {:nemeses nemeses})]
    (reify checker/Checker
      (check [_ test history opts]
        (checker/check c test
                       (h/remove (h/has-f? #{:assign :crash}) history)
                       opts)))))

(defn krabka-test
  "A test map from parsed options."
  [opts]
  (let [workload (kafka/workload opts)
        db       (db/db)
        nemesis  (nemesis-package opts db)
        ; The highest offset seen of each key, which the final polls read up
        ; to. The workload tracks these inside its main generator, which
        ; stops hearing of completions once the main phase ends, so a send
        ; that completes after that is never polled for and is reported
        ; unseen. Tracking around the whole generator counts it.
        offsets  (atom (sorted-map))
        final    (gen/each-thread (kafka/final-polls offsets))]
    (merge tests/noop-test
           opts
           {:name      (test-name opts)
            :db        db
            :os        (if (:docker opts) os/noop debian/os)
            :remote    (if (:docker opts) docker/remote c/ssh)
            ; :nemesis becomes the nemesis itself below; keep the faults.
            :faults    (set (:nemesis opts))
            :node-ips  (atom {})
            :client    (client/client)
            :nemesis   (:nemesis nemesis)
            :generator (kafka/track-key-offsets
                         offsets
                         (gen/phases
                           (->> (:generator workload)
                                (gen/stagger (/ (:rate opts)))
                                (gen/nemesis (:generator nemesis))
                                (gen/time-limit (:time-limit opts)))
                           (gen/nemesis (:final-generator nemesis))
                           (gen/log "Waiting for recovery")
                           (gen/sleep 10)
                           ; A consumer may never see some offsets; give up
                           ; eventually and let the checker say which.
                           (gen/time-limit (:final-time-limit opts)
                                           (gen/clients final))))
            :checker   (checker/compose
                         {:stats    (stats-checker)
                          :perf     (perf-checker (:perf nemesis))
                          :ex       (checker/unhandled-exceptions)
                          ; A broker panic is a bug whatever the history says.
                          :clock    (checker/clock-plot)
                          :panic    (checker/log-file-pattern
                                      #"panicked at" "broker.log")
                          :workload (:checker workload)})
            :perf-opts {:nemeses (:perf nemesis)}
            :logging   {:overrides logging-overrides}})))

;; Options

(def validate-non-neg
  [#(and (number? %) (not (neg? %))) "Must be non-negative"])

(def cli-opts
  "Options beyond Jepsen's own."
  [[nil "--abort-p PROBABILITY" "In a transaction, the probability of an intentional abort at each step."
    :default 0.05
    :parse-fn parse-double
    :validate [#(<= 0 % 1) "Must be between 0 and 1"]]

   [nil "--acks ACKS" "The producers' acks: 0, 1 or all. Unset keeps the client default, all."]

   [nil "--auto-offset-reset BEHAVIOR" "The consumers' auto.offset.reset: earliest or latest."]

   [nil "--bin-dir DIR" "Local directory holding the krabka-broker and krabka-format to install."]

   [nil "--crash-clients" "Periodically crash a client, which opens fresh ones."
    :id :crash-clients?]

   [nil "--crash-client-interval SECONDS" "About how long each client runs before a crash."
    :default 30
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--db-targets TARGETS" "Nodes to kill, pause, delay or skew: a comma-separated list of one, primaries, minority, majority, minority-third and all."
    :default [:one :primaries :majority :all]
    :parse-fn parse-comma-kws
    :validate [(partial every? db-targets) (cli/one-of db-targets)]]

   [nil "--default-topic-replications N" "The brokers' default.replication.factor."
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--enable-auto-commit" "Consumers auto-commit offsets."
    :assoc-fn (fn [m _ _] (assoc m :enable-auto-commit true))]

   [nil "--disable-auto-commit" "Consumers do not auto-commit offsets."
    :assoc-fn (fn [m _ _] (assoc m :enable-auto-commit false))]

   [nil "--enable-server-auto-create-topics" "Brokers create a topic a client asks about."
    :assoc-fn (fn [m _ _] (assoc m :enable-server-auto-create-topics true))]

   [nil "--disable-server-auto-create-topics" "Brokers do not create topics on request."
    :assoc-fn (fn [m _ _] (assoc m :enable-server-auto-create-topics false))]

   [nil "--clock-mode MODE" "How clock faults move a node's clock: faketime (each broker reads its wall-clock offset through libfaketime) or system (Jepsen steps the node's system clock). Defaults to faketime with --docker, system without."
    :parse-fn keyword
    :validate [#{:faketime :system} "Must be faketime or system"]]

   [nil "--docker" "The nodes are local Docker containers named after them: run commands with docker exec, and skip OS setup."]

   [nil "--final-time-limit SECONDS" "The longest the final reads may take."
    :default 120
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--group-protocol PROTOCOL" "The consumers' group.protocol: classic or consumer (KIP-848). Unset keeps the client default."
    :validate [#{"classic" "consumer"} "Must be classic or consumer"]]

   [nil "--[no-]idempotence" "Whether producers enable idempotence. Unset keeps the client default."]

   [nil "--intra-txn-delay MS" "About half of all transactions wait up to this long between micro-ops."
    :default 0
    :parse-fn parse-long
    :validate validate-non-neg]

   [nil "--isolation-level LEVEL" "The consumers' isolation.level: read_committed or read_uncommitted."]

   [nil "--key-count N" "How many keys (topic-partitions) are active at once."
    :default 10
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--max-writes-per-key N" "How many writes a key gets before the generator moves on."
    :default 1024
    :parse-fn parse-long
    :validate [pos? "Must be positive"]]

   [nil "--nemesis FAULTS" "Comma-separated faults: partition, kill, pause, packet, clock; or none, standard or all."
    :default #{}
    :parse-fn parse-faults
    :validate [(partial every? faults)
               (str "Faults must be among " faults " or " (cli/one-of special-faults))]]

   [nil "--nemesis-interval SECONDS" "Seconds between nemesis operations."
    :default 15
    :parse-fn parse-double
    :validate [pos? "Must be positive"]]

   [nil "--partition-targets TARGETS" "Partitions to inject: a comma-separated list of one, primaries, minority-third, majority and majorities-ring."
    :default [:one :primaries :majority :majorities-ring]
    :parse-fn parse-comma-kws
    :validate [(partial every? partition-targets) (cli/one-of partition-targets)]]

   [nil "--rate HZ" "Target operations per second, across all workers."
    :default 100
    :parse-fn parse-double
    :validate validate-non-neg]

   [nil "--retries COUNT" "The producers' retries. Unset keeps the client default."
    :parse-fn parse-long]

   ["-s" "--safe" "The settings under which Kafka promises the most: --acks all, --idempotence, --isolation-level read_committed, --auto-offset-reset earliest, --disable-auto-commit, --disable-server-auto-create-topics, --default-topic-replications 3, --retries 1000 and --sub-via assign. Options after -s override it."
    :assoc-fn (fn [m _ _]
                (assoc m
                       :acks "all"
                       :idempotence true
                       :isolation-level "read_committed"
                       :auto-offset-reset "earliest"
                       :enable-auto-commit false
                       :enable-server-auto-create-topics false
                       :default-topic-replications 3
                       :retries 1000
                       :sub-via #{:assign}))]

   [nil "--sub-via STRATEGIES" "How consumers pick partitions: assign, subscribe, or both, comma-separated."
    :default #{:subscribe}
    :parse-fn (comp set parse-comma-kws)
    :validate [#(and (seq %) (every? #{:assign :subscribe} %))
               "Must be assign and/or subscribe"]]

   [nil "--[no-]txn" "Run sends and polls in Kafka transactions."
    :id :txn?]

   [nil "--[no-]ww-deps" "Infer write-write dependencies from offsets."
    :default true]])

(defn opt-fn
  "Checks option combinations Jepsen cannot run."
  [parsed]
  (let [{:keys [options]} parsed]
    (cond-> parsed
      (and (:docker options) (= :system (:clock-mode options))
           (contains? (:nemesis options) :clock))
      (update :errors conj
              "--clock-mode system needs a clock per node; Docker containers share the host's")

      (not (:bin-dir options))
      (update :errors conj "--bin-dir is required")

      ; Docker containers share the host's clock, which Jepsen's own clock
      ; nemesis would step for every node at once.
      (nil? (:clock-mode options))
      (assoc-in [:options :clock-mode] (if (:docker options) :faketime :system)))))

(def test-opt-spec
  (cli/merge-opt-specs cli/test-opt-spec cli-opts))

(defn parse-test-args
  "Parses a test's arguments as the `test` command would. Returns
  {:options ..., :errors [...]}."
  [args]
  (-> (tools.cli/parse-opts args test-opt-spec)
      cli/test-opt-fn
      opt-fn))

;; Suite

(defn- read-json [path]
  (with-open [r (io/reader path)] (json/read r)))

(defn netem-available?
  "Whether the first node's kernel can delay packets, as the packet fault
  needs. `test` is a parsed option map; this runs before any test does."
  [test]
  (let [node (first (:nodes test))]
    (try
      (c/with-ssh (:ssh test)
        (c/with-remote (if (:docker test) docker/remote c/ssh)
          (let [session (c/session node)]
            (try
              (c/with-session node session
                (c/su
                  (c/exec :tc :qdisc :add :dev :lo :root :netem :delay :1ms)
                  (c/exec :tc :qdisc :del :dev :lo :root)))
              (finally (c/disconnect session))))))
      true
      (catch Exception e
        (info "No netem on" node ":" (.getMessage e))
        false))))

(defn unmet-requirement
  "Why an entry cannot run on this cluster, or nil when it can."
  [requires {:keys [netem?]}]
  (some (fn [r]
          (case r
            "netem"          (when-not netem?
                               "the nodes' kernel has no netem qdisc (packet faults)")
            (str "unknown requirement " r)))
        requires))

(defn result-summary
  "The JSON summary of one finished test."
  [name test]
  (let [results  (:results test)
        workload (:workload results)
        valid    (:valid? results)]
    {"name"            name
     "status"          (case valid true "valid", false "invalid", "unknown")
     "detail"          (->> (dissoc results :valid?)
                            (keep (fn [[k v]]
                                    (when (and (map? v) (not (true? (:valid? v))))
                                      (clojure.core/name k))))
                            sort
                            (str/join ", ")
                            (#(if (str/blank? %) "" (str "invalid checkers: " %))))
     "bad_error_types" (mapv clojure.core/name (:bad-error-types workload))
     "error_types"     (mapv clojure.core/name (:error-types workload))
     "store"           (str (store/path test))}))

(defn run-entry!
  "Runs one manifest entry and returns its summary."
  [entry defaults extra-args env]
  (let [name (get entry "name")]
    (if-let [why (unmet-requirement (get entry "requires" []) env)]
      (do (info "Skipping" name ":" why)
          {"name" name, "status" "skipped", "detail" why})
      (let [args   (concat defaults (get entry "args" []) extra-args)
            parsed (parse-test-args args)]
        (if (seq (:errors parsed))
          {"name" name, "status" "crashed"
           "detail" (str "bad arguments: " (str/join "; " (:errors parsed)))}
          (try
            (info "Running" name "with" (pr-str (vec args)))
            (result-summary name (jepsen/run! (krabka-test (:options parsed))))
            (catch Throwable t
              (warn t "Test" name "crashed")
              {"name" name, "status" "crashed"
               "detail" (first (str/split-lines (str t)))})))))))

(defn split-suite-args
  "Takes `--manifest FILE` and `--results FILE` out of the suite's arguments.
  Returns [manifest results other-args]; the other arguments follow every
  entry's own, so `--bin-dir`, `--nodes` and `--docker` apply to each test."
  [args]
  (loop [args args, manifest nil, results nil, other []]
    (if-let [[a b & more] (seq args)]
      (case a
        "--manifest" (recur more b results other)
        "--results"  (recur more manifest b other)
        (recur (rest args) manifest results (conj other a)))
      [manifest results other])))

(defn run-suite!
  "Runs every test of a manifest and writes a JSON summary of each."
  [args]
  (let [[manifest-file results-file extra] (split-suite-args args)]
    (when-not (and manifest-file results-file)
      (println "Usage: suite --manifest FILE --results FILE [test options]")
      (System/exit 2))
    (let [manifest (read-json manifest-file)
          defaults (get manifest "defaults" [])
          probe    (parse-test-args (concat defaults extra))
          _        (when (seq (:errors probe))
                     (doseq [e (:errors probe)] (println e))
                     (System/exit 2))
          probe    (:options probe)
          wants-netem? (some #{"netem"} (mapcat #(get % "requires")
                                                (get manifest "tests")))
          env      {:docker (:docker probe)
                    :netem? (boolean (and wants-netem? (netem-available? probe)))}
          results  (atom [])]
      (doseq [entry (get manifest "tests")]
        (swap! results conj (run-entry! entry defaults extra env))
        ; Rewrite after every test, so an interrupted run keeps what finished.
        (spit results-file
              (json/write-str {"jepsen_version" jepsen-version
                               "results"        @results}
                              :indent true)))
      (info "Suite results in" results-file)
      (shutdown-agents)
      (System/exit 0))))

;; Entry point

(defn -main
  [& args]
  (if (= "suite" (first args))
    (run-suite! (rest args))
    (cli/run! (merge (cli/single-test-cmd {:test-fn  krabka-test
                                           :opt-spec cli-opts
                                           :opt-fn   opt-fn})
                     (cli/serve-cmd))
              args)))
