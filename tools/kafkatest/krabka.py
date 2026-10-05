"""Run Apache Kafka's ducktape system tests against krabka brokers.

`aspect kafka-system-tests` copies this module into a pinned Apache Kafka
checkout as `kafkatest/services/kafka/krabka.py`, and imports it at the end of
that package's `__init__.py`. The import replaces `KafkaService` with
`KrabkaService` in the package and in `kafkatest.services.kafka.kafka`. Thus
every test starts krabka brokers, and no test file changes. The isolated
controller quorum that `KafkaService.__init__` makes for itself is a
`KrabkaService` too.

The module must be in the same directory as `kafka.py`. ducktape renders the
templates of a service from the `templates/` directory beside the file that
defines its class, and `kafka.properties` is in that directory.

`KrabkaService.start_node` renders the `kafka.properties` file that Kafka
would start the node with. It translates that file into the TOML document,
the format command and the command line of krabka. The producers, consumers,
admin tools and checks of each test stay Kafka's.
"""

import json

from ducktape.errors import TimeoutError as DucktapeTimeoutError
from ducktape.utils.util import wait_until

import kafkatest.services.kafka as kafka_package
import kafkatest.services.kafka.kafka as kafka_module
from kafkatest.services.kafka import config_property, quorum
from kafkatest.services.kafka.kafka import KafkaService
from kafkatest.version import DEV_BRANCH

KRABKA_HOME = "/opt/kafka-dev/krabka"
BROKER_BINARY = "krabka-broker"

# The `listener.security.protocol.map` names krabka's `protocol` key spells
# differently.
PROTOCOLS = {
    "PLAINTEXT": "Plaintext",
    "SSL": "Ssl",
    "SASL_PLAINTEXT": "SaslPlaintext",
    "SASL_SSL": "SaslSsl",
}

# The tests come from Kafka trunk and drive trunk clients, so a node of the
# development branch runs in krabka's trunk mode: it serves the API versions and
# finalizes the feature levels that krabka implements from Kafka trunk past the
# latest release, such as KIP-1191's `share.version` 2. A node that a test pins
# to a release keeps krabka's default, which serves exactly the latest release.
# A test that sets one of these keys keeps its own value.
TRUNK_PROPERTIES = {
    "unstable.api.versions.enable": "true",
    "unstable.feature.versions.enable": "true",
}

# Kafka's test remote storage manager keeps remote segments in a local
# directory. krabka's `[remote_storage] storage_dir` backend does the same.
LOCAL_TIERED_STORAGE = "org.apache.kafka.server.log.remote.storage.LocalTieredStorage"
TIERED_STORAGE_DIR = "/mnt/kafka/tiered-storage"

# Kafka's KIP-392 replica selector classes, by krabka's `replica_selector`.
REPLICA_SELECTORS = {
    "org.apache.kafka.common.replica.LeaderSelector": "leader",
    "org.apache.kafka.common.replica.RackAwareReplicaSelector": "rack-aware",
}

# Kafka keys krabka reads under a `[runtime]` name, with the unit suffix the
# value takes there. The pairs are the ones `KAFKA_STATIC_KEYS` in
# crates/broker/src/config/kafka_static_keys.rs names, plus the log and
# replication keys a test overrides. Every Kafka key, mapped or not, is also
# passed through in `[server_properties]`, where the broker reads the keys it
# consults under their Kafka names and ignores the rest.
RUNTIME_KEYS = {
    "broker.heartbeat.interval.ms": ("heartbeat_interval", "ms"),
    "broker.session.timeout.ms": ("heartbeat_timeout", "ms"),
    "controller.quorum.fetch.timeout.ms": ("controller_election_timeout", "ms"),
    "delegation.token.expiry.check.interval.ms": ("delegation_token_expiry_check_interval", "ms"),
    "delegation.token.expiry.time.ms": ("delegation_token_default_renew_period", "ms"),
    "delegation.token.max.lifetime.ms": ("delegation_token_max_lifetime", "ms"),
    "group.consumer.heartbeat.interval.ms": ("consumer_group_heartbeat_interval", "ms"),
    "group.consumer.max.heartbeat.interval.ms": ("consumer_group_max_heartbeat_interval", "ms"),
    "group.consumer.max.session.timeout.ms": ("consumer_group_max_session_timeout", "ms"),
    "group.consumer.max.size": ("consumer_group_max_size", ""),
    "group.consumer.min.heartbeat.interval.ms": ("consumer_group_min_heartbeat_interval", "ms"),
    "group.consumer.min.session.timeout.ms": ("consumer_group_min_session_timeout", "ms"),
    "group.consumer.session.timeout.ms": ("consumer_group_session_timeout", "ms"),
    "group.initial.rebalance.delay.ms": ("classic_group_initial_rebalance_delay", "ms"),
    "group.max.session.timeout.ms": ("classic_group_max_session_timeout", "ms"),
    "group.max.size": ("classic_group_max_size", ""),
    "group.min.session.timeout.ms": ("classic_group_min_session_timeout", "ms"),
    "group.share.delivery.count.limit": ("share_group_delivery_count_limit", ""),
    "group.share.heartbeat.interval.ms": ("share_group_heartbeat_interval", "ms"),
    "group.share.max.delivery.count.limit": ("share_group_max_delivery_count_limit", ""),
    "group.share.max.heartbeat.interval.ms": ("share_group_max_heartbeat_interval", "ms"),
    "group.share.max.record.lock.duration.ms": ("share_group_max_record_lock_duration", "ms"),
    "group.share.max.session.timeout.ms": ("share_group_max_session_timeout", "ms"),
    "group.share.max.size": ("share_group_max_size", ""),
    "group.share.min.delivery.count.limit": ("share_group_min_delivery_count_limit", ""),
    "group.share.min.heartbeat.interval.ms": ("share_group_min_heartbeat_interval", "ms"),
    "group.share.min.record.lock.duration.ms": ("share_group_min_record_lock_duration", "ms"),
    "group.share.min.session.timeout.ms": ("share_group_min_session_timeout", "ms"),
    "group.share.partition.max.record.locks": ("share_group_partition_max_record_locks", ""),
    "group.share.record.lock.duration.ms": ("share_group_record_lock_duration", "ms"),
    "group.share.session.timeout.ms": ("share_group_session_timeout", "ms"),
    "group.streams.acceptable.recovery.lag": ("streams_group_acceptable_recovery_lag", ""),
    "group.streams.heartbeat.interval.ms": ("streams_group_heartbeat_interval", "ms"),
    "group.streams.max.heartbeat.interval.ms": ("streams_group_max_heartbeat_interval", "ms"),
    "group.streams.max.session.timeout.ms": ("streams_group_max_session_timeout", "ms"),
    "group.streams.max.size": ("streams_group_max_size", ""),
    "group.streams.min.heartbeat.interval.ms": ("streams_group_min_heartbeat_interval", "ms"),
    "group.streams.min.session.timeout.ms": ("streams_group_min_session_timeout", "ms"),
    "group.streams.num.standby.replicas": ("streams_group_num_standby_replicas", ""),
    "group.streams.num.warmup.replicas": ("streams_group_num_warmup_replicas", ""),
    "group.streams.session.timeout.ms": ("streams_group_session_timeout", "ms"),
    "group.streams.task.offset.interval.ms": ("streams_group_task_offset_interval", "ms"),
    "leader.imbalance.check.interval.seconds": ("leader_imbalance_check_interval", "s"),
    "log.retention.check.interval.ms": ("log_retention_check_interval", "ms"),
    "log.segment.bytes": ("log_segment_bytes", "B"),
    "max.connections": ("max_connections", ""),
    "max.connections.per.ip": ("max_connections_per_ip", ""),
    "message.max.bytes": ("message_max_bytes", "B"),
    "metadata.log.max.record.bytes.between.snapshots": ("metadata_max_bytes_between_snapshots", "B"),
    "metadata.log.max.snapshot.interval.ms": ("metadata_max_snapshot_interval", "ms"),
    "metadata.log.segment.bytes": ("metadata_log_segment_bytes", "B"),
    "metadata.log.segment.ms": ("metadata_log_segment_roll_interval", "ms"),
    "metadata.max.idle.interval.ms": ("metadata_max_idle_interval", "ms"),
    "metadata.max.retention.bytes": ("metadata_max_retention_bytes", "B"),
    "metadata.max.retention.ms": ("metadata_max_retention", "ms"),
    "min.insync.replicas": ("default_min_insync_replicas", ""),
    "num.replica.fetchers": ("replica_fetchers", ""),
    "offsets.retention.check.interval.ms": ("offsets_retention_check_interval", "ms"),
    "offsets.retention.minutes": ("offsets_retention", "m"),
    "offsets.topic.num.partitions": ("offsets_topic_num_partitions", ""),
    "offsets.topic.replication.factor": ("offsets_topic_replication_factor", ""),
    "offsets.topic.segment.bytes": ("offsets_topic_segment_bytes", "B"),
    "producer.id.expiration.check.interval.ms": ("producer_id_expiration_scan_interval", "ms"),
    "producer.id.expiration.ms": ("producer_id_expiration", "ms"),
    "queued.max.request.bytes": ("queued_max_request_bytes", "B"),
    "remote.log.manager.task.interval.ms": ("remote_log_manager_interval", "ms"),
    "queued.max.requests": ("queued_max_requests", ""),
    "replica.lag.time.max.ms": ("replica_lag_time_max", "ms"),
    "share.coordinator.state.topic.min.isr": ("share_state_min_isr", ""),
    "share.coordinator.state.topic.num.partitions": ("share_state_num_partitions", ""),
    "share.coordinator.state.topic.replication.factor": ("share_state_replication_factor", ""),
    "share.coordinator.state.topic.segment.bytes": ("share_state_segment_bytes", "B"),
    "socket.receive.buffer.bytes": ("socket_receive_buffer", "B"),
    "socket.request.max.bytes": ("socket_request_max", "B"),
    "socket.send.buffer.bytes": ("socket_send_buffer", "B"),
    "transaction.max.timeout.ms": ("transaction_max_timeout", "ms"),
    "transaction.state.log.min.isr": ("transaction_state_min_isr", ""),
    "transaction.state.log.num.partitions": ("transaction_state_num_partitions", ""),
    "transaction.state.log.replication.factor": ("transaction_state_replication_factor", ""),
    "transaction.state.log.segment.bytes": ("transaction_state_segment_bytes", "B"),
}


def parse_properties(text):
    """The `key=value` pairs of a rendered `kafka.properties`, in order."""
    props = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def _listener_map(value):
    """`NAME://host:port,...` as `{NAME: (host, port)}`."""
    listeners = {}
    for entry in filter(None, (part.strip() for part in value.split(","))):
        name, address = entry.split("://", 1)
        host, port = address.rsplit(":", 1)
        listeners[name] = (host, int(port))
    return listeners


def _protocol_map(value):
    pairs = (entry.split(":", 1) for entry in value.split(",") if entry.strip())
    return {name.strip(): protocol.strip() for name, protocol in pairs}


def _runtime_value(key, value, unit):
    """A `[runtime]` value: a bare integer, or a string with its unit.

    A quantity has no negative form. Kafka reads a negative
    `metadata.max.retention.bytes` or `metadata.max.retention.ms` as no limit,
    which krabka's `[runtime]` keys cannot say.
    """
    if unit:
        if int(value) < 0:
            raise ValueError("krabka has no [runtime] form for %s=%s" % (key, value))
        return "%d%s" % (int(value), unit)
    return int(value)


def _controller_mutation_window(props):
    """Kafka's controller mutation quota window: `quota.window.num` samples of
    `controller.quota.window.size.seconds` each."""
    samples = props.get("quota.window.num")
    seconds = props.get("controller.quota.window.size.seconds")
    if samples is None or seconds is None:
        return None
    return "%ds" % (int(samples) * int(seconds))


def _toml_value(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    # A JSON string is a TOML basic string: both escape `"`, `\` and
    # control characters the same way.
    return json.dumps(value)


def _remote_storage(props):
    """The `[remote_storage]` tables for Kafka's KIP-405 tiered storage keys."""
    if props.get("remote.log.storage.system.enable", "false").lower() != "true":
        return []
    manager = props.get("remote.log.storage.manager.class.name")
    if manager != LOCAL_TIERED_STORAGE:
        raise ValueError("krabka's ducktape adapter supports Kafka's LocalTieredStorage only, not %s" % manager)
    lines = ["", "[remote_storage]", "storage_dir = %s" % _toml_value(TIERED_STORAGE_DIR),
             "", "[remote_storage.kafka_metadata]"]
    partitions = props.get("rlmm.config.remote.log.metadata.topic.num.partitions")
    if partitions is not None:
        lines.append("num_partitions = %d" % int(partitions))
    replication = props.get("rlmm.config.remote.log.metadata.topic.replication.factor")
    if replication is not None:
        lines.append("replication = %d" % int(replication))
        # A metadata topic with fewer replicas than the default minimum ISR
        # could never accept a write.
        lines.append("min_isr = %d" % min(2, int(replication)))
    return lines


class NodeConfig:
    """What one krabka node boots from, derived from its `kafka.properties`."""

    def __init__(self, toml, log_dirs, metadata_log_dir, node_id, controller_listen_addr,
                 controller_listener_name, unstable_feature_versions):
        self.toml = toml
        self.log_dirs = log_dirs
        self.metadata_log_dir = metadata_log_dir
        self.node_id = node_id
        self.controller_listen_addr = controller_listen_addr
        # The first of `controller.listener.names`: `kafka-storage format`
        # names each voter endpoint after it, and a leader refuses an
        # `AddRaftVoter` whose endpoints lack its own listener name.
        self.controller_listener_name = controller_listener_name
        # Kafka's `unstable.feature.versions.enable`: `kafka-storage format`
        # reads it from the node's properties, and `krabka-format` takes it as
        # `--unstable-feature-versions-enable`.
        self.unstable_feature_versions = unstable_feature_versions


def node_config(props, trunk=True):
    """Translate one node's Kafka properties into krabka's configuration.

    The translation keeps Kafka's topology: the same node id, roles, listener
    names, ports, advertised hosts, voter set and log directories. Kafka's
    `metadata.log.dir` is krabka's `metadata_log_dir`, and without it both keep
    the metadata log in the first log directory. `trunk` is whether the node
    runs the development branch, and so krabka's trunk mode.
    """
    if trunk:
        props = dict(TRUNK_PROPERTIES, **props)
    node_id = int(props["node.id"])
    roles = [role.strip() for role in props["process.roles"].split(",")]
    controller_names = [
        name.strip()
        for name in props.get("controller.listener.names", "").split(",")
        if name.strip()
    ]
    protocols = _protocol_map(props.get("listener.security.protocol.map", ""))
    bound = _listener_map(props.get("listeners", ""))
    advertised = _listener_map(props.get("advertised.listeners", ""))
    log_dirs = [path.strip() for path in props["log.dirs"].split(",") if path.strip()]
    metadata_log_dir = props.get("metadata.log.dir", "").strip() or None

    lines = [
        "broker_id = %d" % node_id,
        "log_dir = %s" % _toml_value(log_dirs[0]),
        "extra_log_dirs = %s" % _toml_value(log_dirs[1:]),
    ]
    if metadata_log_dir:
        lines.append("metadata_log_dir = %s" % _toml_value(metadata_log_dir))
    voters = props.get("controller.quorum.voters")
    if voters:
        lines.append("controller_quorum_voters = %s" % _toml_value(
            [voter.strip() for voter in voters.split(",") if voter.strip()]))
    bootstrap = props.get("controller.quorum.bootstrap.servers")
    if bootstrap:
        lines.append("bootstrap_servers = %s" % _toml_value(
            [server.strip() for server in bootstrap.split(",") if server.strip()]))
    if "inter.broker.listener.name" in props:
        lines.append("inter_broker_listener_name = %s" % _toml_value(props["inter.broker.listener.name"]))
    if "broker.rack" in props:
        lines.append("rack = %s" % _toml_value(props["broker.rack"]))
    if "replica.selector.class" in props:
        selector = props["replica.selector.class"]
        if selector not in REPLICA_SELECTORS:
            raise ValueError("krabka has no replica selector for the class %s" % selector)
        lines.append("replica_selector = %s" % _toml_value(REPLICA_SELECTORS[selector]))

    controller_listen_addr = None
    for name in controller_names:
        if name in bound:
            if protocols.get(name, "PLAINTEXT") != "PLAINTEXT":
                raise ValueError("krabka's ducktape adapter supports a PLAINTEXT controller listener only, not %s"
                                 % protocols[name])
            controller_listen_addr = "0.0.0.0:%d" % bound[name][1]
            break

    for name, (_host, port) in bound.items():
        if name in controller_names:
            continue
        protocol = protocols.get(name, name)
        if protocol != "PLAINTEXT":
            raise ValueError("krabka's ducktape adapter supports PLAINTEXT listeners only, not %s" % protocol)
        advertised_host, advertised_port = advertised.get(name, (props.get("advertised.host.name", "localhost"), port))
        lines += [
            "",
            "[[listeners]]",
            "name = %s" % _toml_value(name),
            "bind_addr = %s" % _toml_value("0.0.0.0:%d" % port),
            "advertised = %s" % _toml_value("%s:%d" % (advertised_host, advertised_port)),
            "protocol = %s" % _toml_value(PROTOCOLS[protocol]),
        ]

    lines += ["", "[process]", "roles = %s" % _toml_value(roles)]

    # krabka's audit log is an extension Kafka has no counterpart for. Its
    # `__krabka_audit` partition takes a replica and a log directory slot, and
    # the tests check where Kafka places their own partitions.
    lines += ["", "[audit]", "enabled = false"]
    lines += _remote_storage(props)

    runtime = {}
    for key, (field, unit) in RUNTIME_KEYS.items():
        if key in props:
            runtime[field] = _runtime_value(key, props[key], unit)
    window = _controller_mutation_window(props)
    if window:
        runtime["controller_mutation_quota_window"] = window
    lines += ["", "[runtime]"]
    lines += ["%s = %s" % (field, _toml_value(value)) for field, value in sorted(runtime.items())]

    lines += ["", "[server_properties]"]
    lines += ["%s = %s" % (_toml_value(key), _toml_value(value)) for key, value in sorted(props.items())]

    return NodeConfig("\n".join(lines) + "\n", log_dirs, metadata_log_dir, node_id,
                      controller_listen_addr, controller_names[0] if controller_names else None,
                      props.get("unstable.feature.versions.enable", "false").lower() == "true")


class KrabkaService(KafkaService):
    """`KafkaService` whose nodes run krabka-broker instead of `kafka.Kafka`."""

    KRABKA_CONFIG_FILE = "/mnt/kafka/krabka.toml"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Formatting with `--standalone` names one controller the first voter
        # of a dynamic quorum; `KafkaService.start_node` keeps the same flag.
        self.krabka_standalone_formatted = False

    def start_node(self, node, timeout_sec=60, **kwargs):
        """`KafkaService.start_node` with Kafka's format and launch replaced.

        The steps up to the rendered `kafka.properties` mirror Kafka's at the
        pinned revision, so the node's listeners, ports and voter set are the
        ones Kafka itself would have used.
        """
        if node not in self.nodes_to_start:
            return
        node.account.mkdirs(self.PERSISTENT_ROOT)
        # Kafka's log4j writes the broker log to `server.log` in these two
        # directories, and tests read it there. krabka logs to stdout, which
        # the start command appends to `STDOUT_STDERR_CAPTURE`.
        for directory in (self.OPERATIONAL_LOG_INFO_DIR, self.OPERATIONAL_LOG_DEBUG_DIR):
            node.account.ssh("mkdir -p %s && ln -sfn %s %s/server.log" % (
                directory, self.STDOUT_STDERR_CAPTURE, directory))

        self.node_quorum_info = quorum.NodeQuorumInfo(self.quorum_info, node)
        if self.quorum_info.has_controllers:
            for controller_listener in self.controller_listener_name_list(node):
                if self.node_quorum_info.has_controller_role:
                    self.open_port(controller_listener)
                else:
                    self.close_port(controller_listener)

        self.security_config.setup_node(node)

        security_protocol_to_use = self.controller_quorum.controller_security_protocol
        controller_port = config_property.FIRST_CONTROLLER_PORT + self.SECURITY_PROTOCOLS.index(security_protocol_to_use)
        controllers = self.controller_quorum.nodes[:self.controller_quorum.num_nodes_controller_role]
        if self.dynamicRaftQuorum:
            self.controller_quorum_bootstrap_servers = ",".join(
                "%s:%d" % (controller.account.hostname, controller_port) for controller in controllers)
        else:
            first_node_id = 1 if self.quorum_info.has_brokers_and_controllers else config_property.FIRST_CONTROLLER_ID
            self.controller_quorum_voters = ",".join(
                "%d@%s:%d" % (self.controller_quorum.idx(controller) + first_node_id - 1,
                              controller.account.hostname, controller_port)
                for controller in controllers)
        self.controller_listener_names = ",".join(self.controller_listener_name_list(node))
        if self.isolated_controller_quorum:
            self.controller_sasl_mechanism = self.isolated_controller_quorum.controller_sasl_mechanism

        prop_file = self.prop_file(node)
        self.logger.info("kafka.properties:\n%s" % prop_file)
        node.account.create_file(self.CONFIG_FILE, prop_file)
        config = node_config(parse_properties(prop_file), trunk=node.version == DEV_BRANCH)
        self.logger.info("krabka.toml:\n%s" % config.toml)
        node.account.create_file(self.KRABKA_CONFIG_FILE, config.toml)
        # Kafka's LocalTieredStorage creates its directory. krabka's
        # `storage_dir` backend expects the operator to have made it.
        node.account.mkdirs(TIERED_STORAGE_DIR)

        node.account.ssh(self.format_cmd(node, config))

        cmd = self.krabka_start_cmd(config)
        self.logger.debug("Attempting to start KrabkaService %s on %s with command: %s" %
                          ("concurrently" if self.concurrent_start else "serially", str(node.account), cmd))
        if self.node_quorum_info.has_controller_role and self.node_quorum_info.has_broker_role:
            self.combined_nodes_started += 1
        if self.concurrent_start:
            node.account.ssh(cmd)
        else:
            with node.account.monitor_log(self.STDOUT_STDERR_CAPTURE) as monitor:
                node.account.ssh(cmd)
                self.wait_for_start(node, monitor, timeout_sec)

    def format_cmd(self, node, config):
        """`krabka-format` with the flags `kafka-storage.sh format` gets.

        `kafka-storage.sh format` formats `metadata.log.dir` together with
        `log.dirs`, and so does `--metadata-log-dir`.
        """
        cmd = "%s/krabka-format --ignore-formatted --log-dir %s --cluster-id %s --node-id %d" % (
            KRABKA_HOME, ",".join(config.log_dirs), config_property.CLUSTER_ID, config.node_id)
        if config.unstable_feature_versions:
            cmd += " --unstable-feature-versions-enable"
        if config.metadata_log_dir:
            cmd += " --metadata-log-dir %s" % config.metadata_log_dir
        if self.dynamicRaftQuorum and self.node_quorum_info.has_controller_role:
            if self.krabka_standalone_formatted:
                cmd += " --no-initial-controllers"
            else:
                cmd += " --standalone --controller-listener %s:%s --controller-listener-name %s" % (
                    node.account.hostname, config.controller_listen_addr.rsplit(":", 1)[1],
                    config.controller_listener_name)
                self.krabka_standalone_formatted = True
        cmd += " --feature transaction.version=%d" % (2 if self.use_transactions_v2 else 0)
        if self.share_version is not None:
            cmd += " --feature share.version=%s" % self.share_version
        return cmd + " 1>> %s 2>&1" % self.STDOUT_STDERR_CAPTURE

    def krabka_start_cmd(self, config):
        cmd = "%s/%s --config-file %s --metrics-listen-addr none --health-listen-addr none" % (
            KRABKA_HOME, BROKER_BINARY, self.KRABKA_CONFIG_FILE)
        if config.controller_listen_addr:
            cmd += " --controller-listen-addr %s" % config.controller_listen_addr
        return "%s 1>> %s 2>&1 &" % (cmd, self.STDOUT_STDERR_CAPTURE)

    def wait_for_start(self, node, monitor, timeout_sec=60):
        # A broker that exits during startup never logs the line. Fail when
        # its process is gone, with the error it printed, and not at the end
        # of a timeout that can be ten minutes long.
        def listening():
            try:
                monitor.wait_until("krabka-broker listening", timeout_sec=1, backoff_sec=.25)
                return True
            except DucktapeTimeoutError:
                if not self.pids(node):
                    raise Exception("krabka-broker exited during startup on %s: %s" % (
                        node.account.hostname, self.startup_error(node)))
                return False

        wait_until(listening, timeout_sec=timeout_sec, backoff_sec=0,
                   err_msg="krabka-broker didn't finish startup in %d seconds" % timeout_sec)
        if not self.pids(node):
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

    def startup_error(self, node):
        """The last `Error:` line the broker printed on this node."""
        line = node.account.ssh_output("grep '^Error:' %s | tail -1 || true" % self.STDOUT_STDERR_CAPTURE,
                                       allow_fail=True)
        if isinstance(line, bytes):
            line = line.decode(errors="replace")
        return line.strip() or "no error line"

    def pids(self, node):
        # `-x` matches the process name exactly, so neither this pgrep nor
        # the shell that launched the broker matches.
        return [int(pid) for pid in node.account.ssh_capture("pgrep -x %s || true" % BROKER_BINARY)
                if pid.strip().isdigit()]

    def java_class_name(self):
        # `clean_node` and the fault-injection tests find the server by this
        # name in the process table.
        return BROKER_BINARY

    def thread_dump(self, node):
        # Kafka sends SIGQUIT for a JVM thread dump; it would kill krabka.
        pass


kafka_module.KafkaService = KrabkaService
kafka_package.KafkaService = KrabkaService
