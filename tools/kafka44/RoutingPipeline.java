import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.requests.*;

/** Measures whether a routing error fences requests already pipelined behind it. */
public class RoutingPipeline {
    static void send(DataOutputStream output, AbstractRequest request, int correlation) throws Exception {
        var header = new RequestHeader(request.apiKey(), request.version(), "kafka44-routing-pipeline", correlation);
        ByteBuffer bytes = request.serializeWithHeader(header);
        output.writeInt(bytes.remaining());
        output.write(bytes.array(), bytes.position(), bytes.remaining());
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !List.of("wrong-node", "wrong-cluster").contains(args[3])) throw new IllegalArgumentException("ENDPOINT NODE TOPIC wrong-node|wrong-cluster");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[0]);
        properties.put("default.api.timeout.ms", "15000");
        properties.put("request.timeout.ms", "10000");
        try (Admin admin = Admin.create(properties)) {
            if (admin.listTopics().names().get().contains(args[2])) throw new IllegalStateException("topic already exists");
            String cluster = admin.describeCluster().clusterId().get();
            var handshake = new ApiVersionsRequest(new ApiVersionsRequestData()
                .setClientSoftwareName("kafka44-routing-pipeline").setClientSoftwareVersion("4.4.0")
                .setClusterId(args[3].equals("wrong-cluster") ? cluster + "-stale" : cluster)
                .setNodeId(Integer.parseInt(args[1]) + (args[3].equals("wrong-node") ? 1000 : 0)), (short) 5);
            var mutation = new CreateTopicsRequest.Builder(new CreateTopicsRequestData()
                .setTimeoutMs(10000).setTopics(new CreateTopicsRequestData.CreatableTopicCollection(List.of(
                    new CreateTopicsRequestData.CreatableTopic().setName(args[2]).setNumPartitions(1)
                        .setReplicationFactor((short) 1)).iterator()))).build((short) 7);
            String[] address = args[0].split(":");
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(address[0], Integer.parseInt(address[1])), 10000);
                socket.setSoTimeout(15000);
                var output = new DataOutputStream(socket.getOutputStream());
                send(output, handshake, 1);
                send(output, mutation, 2);
                output.flush();
                var input = new DataInputStream(socket.getInputStream());
                for (int correlation = 1; correlation <= 2; correlation++) {
                    try {
                        int size = input.readInt();
                        if (size < 4 || size > 1048576) throw new IllegalStateException("bad frame size " + size);
                        byte[] bytes = input.readNBytes(size);
                        if (bytes.length != size) throw new IllegalStateException("truncated response");
                        var request = correlation == 1 ? handshake : mutation;
                        var response = AbstractResponse.parseResponse(ByteBuffer.wrap(bytes),
                            new RequestHeader(request.apiKey(), request.version(), "kafka44-routing-pipeline", correlation));
                        System.out.println("RESPONSE\t" + correlation + "\t" + response.data());
                        if (correlation == 2 && ((CreateTopicsResponse) response).data().topics().find(args[2]).errorCode() != 0)
                            throw new IllegalStateException("pipelined create failed");
                        if (correlation == 1 && ((ApiVersionsResponse) response).data().errorCode()
                                != org.apache.kafka.common.protocol.Errors.REBOOTSTRAP_REQUIRED.code())
                            throw new IllegalStateException("routing mismatch was not rejected");
                    } catch (java.io.EOFException closed) {
                        if (correlation == 1) throw closed;
                        throw new IllegalStateException("connection closed unlike released4.4", closed);
                    }
                }
            }
            boolean created = admin.listTopics().names().get().contains(args[2]);
            System.out.println("OBSERVATION\tpipelined-topic-created\t" + created);
            if (!created) throw new IllegalStateException("pipelined mutation was not applied");
            System.out.println("SCENARIO\t" + args[3] + "-does-not-fence-pipeline\tPASS");
        }
    }
}
