import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

/** Explicitly checks the identity of a replacement cluster on the same endpoint. */
public class RoutingReplacement {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) throw new IllegalArgumentException("ENDPOINT NODE STALE_CLUSTER_OR_DASH");
        var metadata = (MetadataResponse) ProtocolScenarios.exchange(args[0],
            MetadataRequest.Builder.allTopics().build((short) 12));
        String current = metadata.data().clusterId();
        if (current == null || current.isEmpty()) throw new IllegalStateException("missing actual cluster identity");
        System.out.println("CLUSTER\t" + current);
        RoutingEndpoints.check("replacement-current", args[0], current, Integer.parseInt(args[1]), (short) 0);
        if (!args[2].equals("-")) {
            if (current.equals(args[2])) throw new IllegalStateException("cluster was not replaced");
            RoutingEndpoints.check("replacement-stale", args[0], args[2], Integer.parseInt(args[1]),
                org.apache.kafka.common.protocol.Errors.REBOOTSTRAP_REQUIRED.code());
        }
    }
}
