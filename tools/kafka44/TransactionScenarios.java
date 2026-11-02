import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;

/** Positive transactional-offset state changes using explicit released v6 codecs. */
public class TransactionScenarios {
    private static AbstractResponse call(String name, String endpoint, AbstractRequest request) throws Exception {
        return call(name, endpoint, request, Errors.NONE);
    }

    private static AbstractResponse call(String name, String endpoint, AbstractRequest request, Errors expected) throws Exception {
        for (int attempt = 0; attempt < 60; attempt++) {
            AbstractResponse response = ProtocolScenarios.exchange(endpoint, request);
            TreeMap<String, Integer> errors = new TreeMap<>();
            response.errorCounts().forEach((error, count) -> errors.put(error.name(), count));
            System.out.println("WIRE\t" + name + "\t" + request.apiKey() + "/v" + request.version() + "\tattempt=" + attempt + "\t" + response.data());
            if (response instanceof TxnOffsetCommitResponse offsets && (offsets.data().topics().size() != 1 || offsets.data().topics().get(0).partitions().size() != 1)) {
                throw new IllegalStateException("transactional offset response lost a row");
            }
            if (response.errorCounts().keySet().stream().allMatch(error -> error == expected)) {
                System.out.println("SCENARIO\t" + name + "\t" + errors);
                return response;
            }
            if (response.errorCounts().keySet().stream().allMatch(error -> error == Errors.NONE || error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.COORDINATOR_NOT_AVAILABLE || error == Errors.NOT_COORDINATOR)) {
                Thread.sleep(100);
            } else {
                throw new IllegalStateException(name + " failed " + errors);
            }
        }
        throw new IllegalStateException(name + " coordinator did not become ready");
    }

    private static InitProducerIdResponseData initialize(String endpoint, String id) throws Exception {
        InitProducerIdRequest request = new InitProducerIdRequest.Builder(new InitProducerIdRequestData()
            .setTransactionalId(id).setTransactionTimeoutMs(60000).setProducerId(-1).setProducerEpoch((short) -1)).build((short) 5);
        return ((InitProducerIdResponse) call(id.endsWith("-abort") ? "abort-init-v5" : "commit-init-v5", endpoint, request)).data();
    }

    private static TxnOffsetCommitRequest offsetRequest(String id, String group, Uuid topic, String topicName, InitProducerIdResponseData producer, long offset, short version, int generation) {
        TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic row = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
            .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                .setPartitionIndex(0).setCommittedOffset(offset).setCommittedLeaderEpoch(-1).setCommittedMetadata("fixture")));
        if (version >= 6) row.setTopicId(topic); else row.setName(topicName);
        return new TxnOffsetCommitRequest(new TxnOffsetCommitRequestData()
            .setTransactionalId(id).setGroupId(group).setProducerId(producer.producerId()).setProducerEpoch(producer.producerEpoch())
            .setGenerationIdOrMemberEpoch(generation).setMemberId("").setTopics(List.of(row)), version);
    }

    private static void offsets(String name, String endpoint, String id, String group, Uuid topic, String topicName, InitProducerIdResponseData producer, long offset, short version) throws Exception {
        TxnOffsetCommitResponse response = (TxnOffsetCommitResponse) call(name, endpoint, offsetRequest(id, group, topic, topicName, producer, offset, version, -1));
        if (version >= 6 && !response.data().topics().get(0).topicId().equals(topic)) throw new IllegalStateException("v6 response did not echo the topic UUID");
    }

    private static String coordinator(String name, String endpoint, String key, byte type) throws Exception {
        FindCoordinatorResponse response = (FindCoordinatorResponse) call(name, endpoint, new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
            .setKeyType(type).setCoordinatorKeys(List.of(key))).build((short) 4));
        FindCoordinatorResponseData.Coordinator coordinator = response.coordinatorByKey(key).orElseThrow();
        if (coordinator.nodeId() < 0 || coordinator.host().isEmpty() || coordinator.port() < 1 || coordinator.port() > 65535) throw new IllegalStateException("invalid coordinator endpoint");
        return coordinator.host() + ":" + coordinator.port();
    }

    private static void finish(String name, String endpoint, String id, InitProducerIdResponseData producer, boolean commit) throws Exception {
        call(name, endpoint, new EndTxnRequest.Builder(new EndTxnRequestData().setTransactionalId(id)
            .setProducerId(producer.producerId()).setProducerEpoch(producer.producerEpoch()).setCommitted(commit), true).build((short) 5));
    }

    private static Long read(Admin admin, String group, TopicPartition partition, boolean stable) throws Exception {
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group,
                new ListConsumerGroupOffsetsOptions().requireStable(stable)).partitionsToOffsetAndMetadata().get();
            OffsetAndMetadata offset = offsets.get(partition);
            return offset == null ? null : offset.offset();
        } catch (java.util.concurrent.ExecutionException error) {
            if (error.getCause() instanceof GroupIdNotFoundException) return null;
            throw error;
        }
    }

    private static void observed(String name, Admin admin, String group, TopicPartition partition, Long expected, boolean stable) throws Exception {
        for (int attempt = 0; attempt < 60; attempt++) {
            Long actual = read(admin, group, partition, stable);
            System.out.println("OBSERVATION\t" + name + "\t" + actual + "\tstable=" + stable + "\tattempt=" + attempt);
            if (java.util.Objects.equals(actual, expected)) {
                System.out.println("SCENARIO\t" + name + "\tPASS");
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException(name + ": expected " + expected + " did not become visible");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1 || args.length > 5) throw new IllegalArgumentException("isolated broker endpoint required");
        String endpoint = args[0];
        short offsetVersion = args.length == 2 && args[1].equals("legacy") ? (short) 5 : (short) 6;
        if (args.length == 5 && args[1].equals("recover")) {
            Properties recoveryConfig = new Properties();
            recoveryConfig.put("bootstrap.servers", endpoint);
            recoveryConfig.put("default.api.timeout.ms", "30000");
            try (Admin admin = Admin.create(recoveryConfig)) {
                Uuid actual = admin.describeTopics(List.of(args[2])).allTopicNames().get().get(args[2]).topicId();
                if (!actual.equals(Uuid.fromString(args[4]))) throw new IllegalStateException("restart changed topic incarnation");
                System.out.println("SCENARIO\trestart-preserves-topic-id\tPASS");
                observed("restart-preserves-commit-not-abort", admin, args[3], new TopicPartition(args[2], 0), 7L, true);
            }
            return;
        }
        if (args.length > 2 || (args.length == 2 && !args[1].equals("legacy"))) throw new IllegalArgumentException("invalid fixture mode");
        String suffix = java.util.UUID.randomUUID().toString();
        String topic = "kafka44-txn-" + suffix;
        String group = "kafka44-group-" + suffix;
        Properties config = new Properties();
        config.put("bootstrap.servers", endpoint);
        config.put("request.timeout.ms", "10000");
        config.put("default.api.timeout.ms", "30000");
        try (Admin admin = Admin.create(config)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            Uuid topicId = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
            if (topicId.equals(Uuid.ZERO_UUID)) throw new IllegalStateException("missing topic UUID");
            TopicPartition partition = new TopicPartition(topic, 0);
            System.out.println("SETUP\ttopic=" + topic + "\ttopicId=" + topicId + "\tgroup=" + group);
            // Warm both coordinators without mutating group membership.
            String groupEndpoint = coordinator("group-coordinator", endpoint, group, (byte) 0);
            String missingGroupEndpoint = coordinator("missing-group-coordinator", endpoint, group + "-missing", (byte) 0);
            String committed = "kafka44-" + suffix + "-commit";
            String commitEndpoint = coordinator("commit-coordinator", endpoint, committed, (byte) 1);
            InitProducerIdResponseData commitProducer = initialize(commitEndpoint, committed);
            if (offsetVersion >= 6) call("missing-group-v6", missingGroupEndpoint, offsetRequest(committed, group + "-missing", topicId, topic, commitProducer, 3, (short) 6, 3), Errors.GROUP_ID_NOT_FOUND);
            call("missing-group-legacy-v5", missingGroupEndpoint, offsetRequest(committed, group + "-missing", topicId, topic, commitProducer, 3, (short) 5, 3), Errors.ILLEGAL_GENERATION);
            offsets("commit-offset-v" + offsetVersion, groupEndpoint, committed, group, topicId, topic, commitProducer, 7, offsetVersion);
            observed("pending-commit-invisible", admin, group, partition, null, false);
            finish("commit-end-v5", commitEndpoint, committed, commitProducer, true);
            observed("committed-offset-visible", admin, group, partition, 7L, true);
            String aborted = "kafka44-" + suffix + "-abort";
            String abortEndpoint = coordinator("abort-coordinator", endpoint, aborted, (byte) 1);
            InitProducerIdResponseData abortProducer = initialize(abortEndpoint, aborted);
            offsets("abort-offset-v" + offsetVersion, groupEndpoint, aborted, group, topicId, topic, abortProducer, 42, offsetVersion);
            observed("pending-abort-keeps-old-offset", admin, group, partition, 7L, false);
            finish("abort-end-v5", abortEndpoint, aborted, abortProducer, false);
            observed("aborted-offset-not-visible", admin, group, partition, 7L, true);
            System.out.println("RECOVERY\t" + topic + "\t" + group + "\t" + topicId);
        }
    }
}
