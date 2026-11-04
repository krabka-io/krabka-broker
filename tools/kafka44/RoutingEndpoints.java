import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;

/** Explicit endpoint identities: caller owns provisioning and restart between invocations. */
public class RoutingEndpoints {
    static void check(String name, String endpoint, String cluster, int node, short expected) throws Exception {
        var request = new ApiVersionsRequest(new ApiVersionsRequestData()
            .setClientSoftwareName("kafka44-routing-endpoints").setClientSoftwareVersion("4.4.0")
            .setClusterId(cluster).setNodeId(node), (short) 5);
        var response = (ApiVersionsResponse) ProtocolScenarios.exchange(endpoint, request);
        System.out.println("RESPONSE\t" + name + "\t" + response.data());
        if (response.data().errorCode() != expected)
            throw new IllegalStateException(name + ": expected " + expected + " got " + response.data().errorCode());
        System.out.println("SCENARIO\t" + name + "\tPASS");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 5) throw new IllegalArgumentException("ENDPOINT1 NODE1 ENDPOINT2 NODE2 PHASE");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[0]);
        properties.put("default.api.timeout.ms", "15000");
        properties.put("request.timeout.ms", "10000");
        try (Admin admin = Admin.create(properties)) {
            String cluster = admin.describeCluster().clusterId().get();
            int first = Integer.parseInt(args[1]), second = Integer.parseInt(args[3]);
            if (first == second || args[0].equals(args[2])) throw new IllegalArgumentException("distinct endpoints and node IDs required");
            System.out.println("CLUSTER\t" + cluster);
            for (int i = 0; i < 2; i++) {
                String endpoint = args[i * 2];
                int local = i == 0 ? first : second, other = i == 0 ? second : first;
                String prefix = args[4] + "-node" + local;
                check(prefix + "-correct", endpoint, cluster, local, (short) 0);
                check(prefix + "-wrong-node", endpoint, cluster, other, org.apache.kafka.common.protocol.Errors.REBOOTSTRAP_REQUIRED.code());
                check(prefix + "-wrong-cluster", endpoint, "stale-cluster", local, org.apache.kafka.common.protocol.Errors.REBOOTSTRAP_REQUIRED.code());
                check(prefix + "-partial", endpoint, cluster, -1, org.apache.kafka.common.protocol.Errors.INVALID_REQUEST.code());
                check(prefix + "-absent", endpoint, null, -1, (short) 0);
            }
        }
    }
}
