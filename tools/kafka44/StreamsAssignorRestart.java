import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Run prepare, restart with only sticky registered, then run verify on the same data. */
public class StreamsAssignorRestart extends StreamsAssignorWire {
    static void configured(Admin admin, String expected) throws Exception {
        var resource = new ConfigResource(ConfigResource.Type.GROUP, group);
        var value = admin.describeConfigs(List.of(resource)).all().get().get(resource)
            .get("streams.assignor.name").value();
        require(expected.equals(value), "persisted override: " + value);
        System.out.println("SCENARIO\tpersisted-override-" + expected + "\tPASS");
    }
    public static void main(String[] args) throws Exception {
        ep = args[0]; group = "restart-assignor-" + args[1]; topic = "restart-source-" + args[1];
        Properties p = new Properties(); p.put("bootstrap.servers", ep);
        p.put("default.api.timeout.ms", "20000"); p.put("request.timeout.ms", "10000");
        try (Admin admin = Admin.create(p)) {
            call(new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKeyType((byte)0)
                .setCoordinatorKeys(List.of(group))).build((short)4));
            if (args[2].equals("prepare")) {
                admin.createTopics(List.of(new NewTopic(topic, 2, (short)1))).all().get();
                join("a"); select(admin, "last"); join("b");
                target("last", "b"); reconcile("b"); configured(admin, "last");
                for (var m : describe().members()) {
                    var response = ((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(
                        new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId(m.memberId())
                            .setMemberEpoch(-1), (short)1))).data();
                    require(response.errorCode() == 0, response);
                }
                require(describe().members().isEmpty(), "members did not leave");
                System.out.println("SCENARIO\tprepared-empty-group\tPASS");
            } else if (args[2].equals("verify")) {
                configured(admin, "last");
                join("c"); target("sticky", "c"); reconcile("c");
                configured(admin, "last");
                System.out.println("SCENARIO\tunavailable-persisted-name-falls-back\tPASS");
                select(admin, "sticky"); configured(admin, "sticky");
                join("d");
                require(describe().assignorName().equals("sticky"), "repair changed assignor");
                System.out.println("SCENARIO\trepair-override-after-restart\tPASS");
            } else throw new IllegalArgumentException("expected prepare or verify");
        }
    }
}
