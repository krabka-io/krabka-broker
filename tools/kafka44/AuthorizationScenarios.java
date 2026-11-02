import java.util.List;
import java.util.TreeMap;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Dedicated synthetic user with no ACLs; never use production endpoints. */
public class AuthorizationScenarios {
    private static boolean failed;

    private static void probe(String name, String endpoint, AbstractRequest request, String user) {
        try {
            AbstractResponse response = user != null
                ? ProtocolScenarios.exchange(endpoint, request, user, "qualification-fixture-password")
                : ProtocolScenarios.exchange(endpoint, request);
            TreeMap<String, Integer> errors = new TreeMap<>();
            response.errorCounts().forEach((error, count) -> errors.put(error.name(), count));
            System.out.println("SCENARIO\t" + name + "\t" + errors);
            System.out.println("RESPONSE\t" + name + "\t" + response.data());
        } catch (Exception error) {
            failed = true;
            System.out.println("SCENARIO\t" + name + "\tTRANSPORT:" + error.getClass().getSimpleName());
        }
    }

    private static AclBinding grant(String principal, ResourceType type, String resource, AclOperation operation) {
        return new AclBinding(new ResourcePattern(type, resource, PatternType.LITERAL),
            new AccessControlEntry("User:" + principal, "*", operation, AclPermissionType.ALLOW));
    }

    private static void grantFixtureRoles(String bootstrap) throws Exception {
        Properties config = new Properties();
        config.put("bootstrap.servers", bootstrap);
        config.put("request.timeout.ms", "10000");
        config.put("default.api.timeout.ms", "15000");
        List<AclBinding> bindings = List.of(
            grant("operator", ResourceType.CLUSTER, "kafka-cluster", AclOperation.ALTER),
            grant("operator", ResourceType.GROUP, "kafka44-denied", AclOperation.READ),
            grant("writer", ResourceType.TRANSACTIONAL_ID, "kafka44-denied", AclOperation.WRITE),
            grant("reader", ResourceType.TRANSACTIONAL_ID, "kafka44-denied", AclOperation.WRITE),
            grant("reader", ResourceType.GROUP, "kafka44-denied", AclOperation.READ));
        try (Admin admin = Admin.create(config)) {
            admin.createAcls(bindings).all().get();
            for (int attempt = 0; attempt < 30; attempt++) {
                if (admin.describeAcls(AclBindingFilter.ANY).values().get().containsAll(bindings)) {
                    System.out.println("SETUP\tACL-visible-attempt\t" + attempt + "\t" + bindings);
                    return;
                }
                Thread.sleep(100);
            }
            throw new IllegalStateException("fixture ACL propagation did not complete");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("isolated SASL and privileged plaintext broker endpoints required");
        String broker = args[0];
        grantFixtureRoles(args[1]);
        ApiVersionsRequest routing = new ApiVersionsRequest(new ApiVersionsRequestData()
            .setClientSoftwareName("kafka44-qualification").setClientSoftwareVersion("1")
            .setClusterId("wrong-cluster").setNodeId(1), (short) 5);
        probe("preauth-routing-identity", broker, routing, null);
        probe("authenticated-routing-identity", broker, routing, "denied");
        probe("controller-cluster-authorization", broker, new UnregisterControllerRequest(
            new UnregisterControllerRequestData().setControllerId(424242), (short) 0), "denied");
        probe("topology-group-authorization", broker, new StreamsGroupTopologyDescriptionUpdateRequest(
            new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId("kafka44-denied")
                .setMemberId("missing-member").setTopologyEpoch(1), (short) 0), "denied");
        probe("streams-group-authorization", broker, new StreamsGroupHeartbeatRequest(
            new StreamsGroupHeartbeatRequestData().setGroupId("kafka44-denied").setMemberId("missing-member").setMemberEpoch(1), (short) 1), "denied");
        probe("txn-transactional-id-authorization", broker, new TxnOffsetCommitRequest(
            new TxnOffsetCommitRequestData().setTransactionalId("kafka44-denied")
                .setGroupId("kafka44-denied").setProducerId(-1).setProducerEpoch((short) -1)
                .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                    .setTopicId(new Uuid(0, 42)).setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                        .setPartitionIndex(0).setCommittedOffset(1))))), (short) 6), "denied");
        probe("controller-authorized-unknown-id", broker, new UnregisterControllerRequest(
            new UnregisterControllerRequestData().setControllerId(2147483646), (short) 0), "operator");
        probe("topology-authorized-no-plugin", broker, new StreamsGroupTopologyDescriptionUpdateRequest(
            new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId("kafka44-denied")
                .setMemberId("missing-member").setTopologyEpoch(1), (short) 0), "operator");
        TxnOffsetCommitRequest unknownTopic = new TxnOffsetCommitRequest(new TxnOffsetCommitRequestData()
            .setTransactionalId("kafka44-denied").setGroupId("kafka44-denied").setProducerId(-1).setProducerEpoch((short) -1)
            .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setTopicId(new Uuid(0, 42))
                .setPartitions(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                    .setPartitionIndex(0).setCommittedOffset(1))))), (short) 6);
        probe("txn-group-authorization-after-tid-allow", broker, unknownTopic, "writer");
        probe("txn-unknown-id-after-tid-group-allow", broker, unknownTopic, "reader");
        if (failed) System.exit(1);
    }
}
