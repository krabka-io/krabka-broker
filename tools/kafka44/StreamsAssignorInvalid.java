import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Characterizes invalid callback output; expected error is explicit, not assumed parity. */
public class StreamsAssignorInvalid extends StreamsAssignorWire {
    public static void main(String[] args) throws Exception {
        ep = args[0]; group = "invalid-assignor-" + args[1]; topic = "invalid-source-" + args[1];
        short expectedError = Short.parseShort(args[2]);
        Properties p = new Properties(); p.put("bootstrap.servers", ep);
        p.put("default.api.timeout.ms", "20000"); p.put("request.timeout.ms", "10000");
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short)1))).all().get();
            call(new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                .setKeyType((byte)0).setCoordinatorKeys(List.of(group))).build((short)4));
            join("a"); target("first", "a"); reconcile("a");
            var before = describe();
            select(admin, "unknown-member");
            // Allow the default assignment interval to elapse before forcing a join.
            Thread.sleep(6000);
            var request = new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId("b")
                .setMemberEpoch(0).setProcessId("b").setRebalanceTimeoutMs(30000)
                .setActiveTasks(List.of()).setStandbyTasks(List.of()).setWarmupTasks(List.of())
                .setTopology(new StreamsGroupHeartbeatRequestData.Topology().setEpoch(0)
                    .setSubtopologies(List.of(new StreamsGroupHeartbeatRequestData.Subtopology()
                        .setSubtopologyId("0").setSourceTopics(List.of(topic)))));
            var response = ((StreamsGroupHeartbeatResponse)call(
                new StreamsGroupHeartbeatRequest(request, (short)1))).data();
            System.out.println("RESPONSE\tunknown-member-output\t" + response);
            require(response.errorCode() == expectedError, response);
            System.out.println("SCENARIO\tinvalid-output-error-" + expectedError + "\tPASS");
            var after = describe();
            if (expectedError != 0) {
                require(after.members().size() == 1 && after.members().get(0).memberId().equals("a"), after);
                require(after.groupEpoch() == before.groupEpoch()
                    && after.assignmentEpoch() == before.assignmentEpoch(), after);
                require(after.members().get(0).assignment().equals(before.members().get(0).assignment())
                    && after.members().get(0).targetAssignment().equals(before.members().get(0).targetAssignment()), after);
                System.out.println("SCENARIO\tinvalid-output-no-partial-state\tPASS");
            } else {
                require(after.members().stream().anyMatch(m -> m.memberId().equals("b")), after);
                System.out.println("SCENARIO\tinvalid-output-member-accepted\tPASS");
            }
            select(admin, "first"); join("c"); target("first", "a"); reconcile("a");
            System.out.println("SCENARIO\tinvalid-output-repair-retry\tPASS");
        }
    }
}
