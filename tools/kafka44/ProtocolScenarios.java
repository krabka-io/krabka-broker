import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import java.util.TreeMap;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Negative wire scenarios using only the immutable released Kafka client's codecs.
 * No result here constitutes qualification: the collector retains oracle differences.
 */
public class ProtocolScenarios {
    private static int correlation = 0;

    private static AbstractResponse exchange(String endpoint, AbstractRequest request) throws Exception {
        String[] address = endpoint.split(":");
        RequestHeader header = new RequestHeader(request.apiKey(), request.version(), "kafka44-qualification", ++correlation);
        ByteBuffer payload = request.serializeWithHeader(header);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(address[0], Integer.parseInt(address[1])), 10000);
            socket.setSoTimeout(15000);
            DataOutputStream output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(payload.remaining());
            output.write(payload.array(), payload.position(), payload.remaining());
            output.flush();
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int size = input.readInt();
            if (size < 4 || size > 1048576) throw new IllegalStateException("invalid response size " + size);
            byte[] bytes = input.readNBytes(size);
            if (bytes.length != size) throw new IllegalStateException("truncated response");
            return AbstractResponse.parseResponse(ByteBuffer.wrap(bytes), header);
        }
    }

    private static void probe(String name, String endpoint, AbstractRequest request) {
        try {
            AbstractResponse response = exchange(endpoint, request);
            TreeMap<String, Integer> errors = new TreeMap<>();
            response.errorCounts().forEach((error, count) -> errors.put(error.name(), count));
            System.out.println("SCENARIO\t" + name + "\t" + errors);
            System.out.println("RESPONSE\t" + name + "\t" + response.data());
        } catch (Exception error) {
            System.out.println("SCENARIO\t" + name + "\tTRANSPORT:" + error.getClass().getSimpleName());
        }
    }

    private static ApiVersionsRequest routing(String cluster, int node) {
        return new ApiVersionsRequest(new ApiVersionsRequestData()
            .setClientSoftwareName("kafka44-qualification").setClientSoftwareVersion("1")
            .setClusterId(cluster).setNodeId(node), (short) 5);
    }

    private static void routingProbes(String broker, String controller) throws Exception {
        Properties config = new Properties();
        config.put("bootstrap.servers", broker);
        config.put("default.api.timeout.ms", "15000");
        config.put("request.timeout.ms", "10000");
        try (Admin admin = Admin.create(config)) {
            String cluster = admin.describeCluster().clusterId().get();
            int node = admin.describeCluster().nodes().get().iterator().next().id();
            probe("routing-no-identity", broker, routing(null, -1));
            probe("routing-cluster-only", broker, routing(cluster, -1));
            probe("routing-node-only", broker, routing(null, node));
            probe("routing-correct-identity", broker, routing(cluster, node));
            probe("routing-wrong-cluster", broker, routing("wrong-cluster", node));
            probe("routing-controller-wrong-cluster", controller, routing("wrong-cluster", node));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("broker and controller endpoints required");
        String broker = args[0];
        String controller = args[1];
        routingProbes(broker, controller);
        probe("controller-unknown-id", controller, new UnregisterControllerRequest(
            new UnregisterControllerRequestData().setControllerId(2147483646), (short) 0));
        probe("controller-api-on-broker", broker, new UnregisterControllerRequest(
            new UnregisterControllerRequestData().setControllerId(2147483646), (short) 0));
        probe("topology-no-plugin", broker, new StreamsGroupTopologyDescriptionUpdateRequest(
            new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId("kafka44-negative")
                .setMemberId("missing-member").setTopologyEpoch(1), (short) 0));
        probe("streams-invalid-group", broker, new StreamsGroupHeartbeatRequest(
            new StreamsGroupHeartbeatRequestData().setGroupId("").setMemberId("missing-member"), (short) 1));
        probe("txn-topic-id-missing-group", broker, new TxnOffsetCommitRequest(
            new TxnOffsetCommitRequestData().setTransactionalId("kafka44-negative")
                .setGroupId("kafka44-missing").setProducerId(-1).setProducerEpoch((short) -1)
                .setTopics(List.of(new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic()
                    .setTopicId(new Uuid(0, 42)).setPartitions(List.of(
                        new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition()
                            .setPartitionIndex(0).setCommittedOffset(1))))), (short) 6));
    }
}
