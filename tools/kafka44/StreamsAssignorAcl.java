import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Synthetic SASL actor: group/topic denial precedes a selected failing callback. */
public class StreamsAssignorAcl extends StreamsAssignorWire {
    static String actorEndpoint;
    static AclBinding grant(ResourceType type, String resource, AclOperation operation) {
        return new AclBinding(new ResourcePattern(type, resource, PatternType.LITERAL),
            new AccessControlEntry("User:actor", "*", operation, AclPermissionType.ALLOW));
    }
    static void probe(short expected, String name) throws Exception {
        var request = new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId("actor")
            .setMemberEpoch(0).setProcessId("actor").setRebalanceTimeoutMs(30000)
            .setActiveTasks(List.of()).setStandbyTasks(List.of()).setWarmupTasks(List.of())
            .setTopology(new StreamsGroupHeartbeatRequestData.Topology().setEpoch(0).setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("0").setSourceTopics(List.of(topic)))));
        var d = ((StreamsGroupHeartbeatResponse)ProtocolScenarios.exchange(actorEndpoint,
            new StreamsGroupHeartbeatRequest(request, (short)1), "actor", "qualification-fixture-password")).data();
        System.out.println("RESPONSE\t" + name + "\t" + d);
        require(d.errorCode() == expected, d);
        require(describe().members().stream().noneMatch(m -> m.memberId().equals("actor")), "failed member persisted");
        System.out.println("SCENARIO\t" + name + "\tPASS");
    }
    public static void main(String[] args) throws Exception {
        ep = args[0]; actorEndpoint = args[1]; group = "assignor-acl-" + args[2]; topic = "acl-source-" + args[2];
        Properties p = new Properties(); p.put("bootstrap.servers", ep); p.put("default.api.timeout.ms", "20000"); p.put("request.timeout.ms", "10000");
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short)1))).all().get();
            call(new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKeyType((byte)0)
                .setCoordinatorKeys(List.of(group))).build((short)4));
            join("a"); target("first", "a"); reconcile("a");
            select(admin, "failure"); Thread.sleep(6000);
            probe((short)30, "group-denial-with-failing-callback");
            admin.createAcls(List.of(grant(ResourceType.GROUP, group, AclOperation.READ))).all().get();
            Thread.sleep(600);
            probe((short)29, "topic-denial-after-group-grant");
            admin.createAcls(List.of(grant(ResourceType.TOPIC, topic, AclOperation.READ))).all().get();
            Thread.sleep(600);
            probe((short)-1, "callback-error-after-acl-grants");
            select(admin, "first"); join("c"); target("first", "a"); reconcile("a");
            System.out.println("SCENARIO\tacl-callback-repair\tPASS");
        }
    }
}
