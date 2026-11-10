import java.util.*;
import java.nio.file.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Active custom-assignment recovery after an externally killed RF3 owner. */
public class StreamsAssignorMovement extends StreamsAssignorWire {
    static FindCoordinatorResponseData.Coordinator owner() throws Exception {
        var d = ((FindCoordinatorResponse)call(new FindCoordinatorRequest.Builder(
            new FindCoordinatorRequestData().setKeyType((byte)0).setCoordinatorKeys(List.of(group)))
                .build((short)4))).data();
        require(d.coordinators().size() == 1 && d.coordinators().get(0).errorCode() == 0, d);
        var c = d.coordinators().get(0);
        System.out.println("OWNER\t" + c.nodeId() + "\t" + c.host() + ":" + c.port());
        return c;
    }
    public static void main(String[] args) throws Exception {
        ep = args[0]; var identity = Path.of(args[1]);
        boolean prepare = args[2].equals("prepare");
        require(prepare || args[2].equals("recover"), "prepare/recover");
        Properties p = new Properties(); p.put("bootstrap.servers", ep);
        p.put("default.api.timeout.ms", "30000");
        if (prepare) {
            String suffix = UUID.randomUUID().toString();
            group = "assignor-movement-" + suffix; topic = "assignor-source-" + suffix;
            try (Admin admin = Admin.create(p)) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short)3)
                    .configs(Map.of("min.insync.replicas", "2")))).all().get();
                var c = owner(); ep = c.host() + ":" + c.port();
                join("a"); target("first", "a"); reconcile("a");
                select(admin, "last"); join("b"); target("last", "b"); reconcile("b");
                var before = describe(); require(before.members().size() == 2, before);
                Files.writeString(identity, group + "\n" + topic + "\n" + c.nodeId() + "\n");
                System.out.println("SCENARIO\tactive-owner-prepared\tPASS");
            }
        } else {
            var lines = Files.readAllLines(identity); require(lines.size() == 3, lines);
            group = lines.get(0); topic = lines.get(1); var c = owner();
            for (int attempt = 0; c.nodeId() == Integer.parseInt(lines.get(2)) && attempt < 80; attempt++) {
                Thread.sleep(250); c = owner();
            }
            require(c.nodeId() != Integer.parseInt(lines.get(2)), "owner did not change");
            ep = c.host() + ":" + c.port();
            System.out.println("SCENARIO\tcoordinator-owner-changed\tPASS");
            var after = describe();
            require(after.members().size() == 2 && after.members().stream()
                .map(m -> m.memberId()).collect(java.util.stream.Collectors.toSet()).equals(Set.of("a", "b")), after);
            target("last", "b"); reconcile("b");
            System.out.println("SCENARIO\tactive-custom-assignment-survived\tPASS");
            try (Admin admin = Admin.create(p)) {
                select(admin, "first"); join("c"); target("first", "a"); reconcile("a");
                System.out.println("SCENARIO\tnew-owner-callback-reassignment\tPASS");
            }
        }
    }
}
