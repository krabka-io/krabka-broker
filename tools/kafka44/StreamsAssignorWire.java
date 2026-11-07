import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Actual custom callbacks on both brokers, using the released 4.4 wire codec. */
public class StreamsAssignorWire extends StreamsPositive {
    static void select(Admin admin, String name) throws Exception {
        var resource = new ConfigResource(ConfigResource.Type.GROUP, group);
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
            new ConfigEntry("streams.assignor.name", name), AlterConfigOp.OpType.SET)))).all().get();
    }
    static void target(String assignor, String owner) throws Exception {
        var g = describe();
        for (int attempt = 0; g.assignmentEpoch() < g.groupEpoch() && attempt < 100; attempt++) {
            Thread.sleep(100);
            for (var member : g.members()) {
                call(new StreamsGroupHeartbeatRequest(new StreamsGroupHeartbeatRequestData()
                    .setGroupId(group).setMemberId(member.memberId()).setMemberEpoch(member.memberEpoch()), (short)1));
            }
            g = describe();
        }
        require(assignor.equals(g.assignorName()), g);
        int count = 0;
        for (var member : g.members()) {
            var tasks = member.targetAssignment().activeTasks();
            if (member.memberId().equals(owner)) {
                require(tasks.size() == 1 && tasks.get(0).subtopologyId().equals("0")
                    && tasks.get(0).partitions().equals(List.of(0, 1)), member);
                count++;
            } else require(tasks.isEmpty(), member);
        }
        require(count == 1, g);
        System.out.println("SCENARIO\ttarget-" + assignor + "-" + owner + "\tPASS");
    }
    static void reconcile(String owner) throws Exception {
        for (int attempt = 0; attempt < 40; attempt++) {
            var g = describe();
            boolean done = g.members().stream().allMatch(member ->
                member.assignment().activeTasks().equals(member.targetAssignment().activeTasks()) &&
                (member.memberId().equals(owner) ? member.assignment().activeTasks().size() == 1
                    && member.assignment().activeTasks().get(0).partitions().equals(List.of(0, 1))
                    : member.assignment().activeTasks().isEmpty()));
            if (done) { System.out.println("SCENARIO\tcooperative-owner-" + owner + "\tPASS"); return; }
            for (var member : g.members()) {
                var retained = new ArrayList<StreamsGroupHeartbeatRequestData.TaskIds>();
                for (var active : member.assignment().activeTasks()) {
                    var allowed = member.targetAssignment().activeTasks().stream()
                        .filter(task -> task.subtopologyId().equals(active.subtopologyId())).findFirst();
                    if (allowed.isPresent()) {
                        var parts = active.partitions().stream().filter(allowed.get().partitions()::contains).toList();
                        if (!parts.isEmpty()) retained.add(new StreamsGroupHeartbeatRequestData.TaskIds()
                            .setSubtopologyId(active.subtopologyId()).setPartitions(parts));
                    }
                }
                var response = ((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(
                    new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId(member.memberId())
                        .setMemberEpoch(member.memberEpoch()).setActiveTasks(retained)
                        .setStandbyTasks(List.of()).setWarmupTasks(List.of()), (short)1))).data();
                require(response.errorCode() == 0, response);
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("cooperative reconciliation timed out");
    }
    public static void main(String[] args) throws Exception {
        ep = args[0]; group = "assignor-" + args[1]; topic = "assignor-source-" + args[1];
        Properties p = new Properties(); p.put("bootstrap.servers", ep);
        p.put("default.api.timeout.ms", "20000"); p.put("request.timeout.ms", "10000");
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, 2, (short)1))).all().get();
            call(new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKeyType((byte)0).setCoordinatorKeys(List.of(group))).build((short)4));
            join("a"); target("first", "a");
            select(admin, "last"); join("b"); target("last", "b"); reconcile("b");
            select(admin, "failure");
            Thread.sleep(1200);
            var request = new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId("c")
                .setMemberEpoch(0).setProcessId("c").setRebalanceTimeoutMs(30000)
                .setActiveTasks(List.of()).setStandbyTasks(List.of()).setWarmupTasks(List.of());
            request.setTopology(new StreamsGroupHeartbeatRequestData.Topology().setEpoch(0).setSubtopologies(List.of(
                new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("0").setSourceTopics(List.of(topic)))));
            var failure = ((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(request,(short)1))).data();
            System.out.println("RESPONSE\tfailure\t" + failure);
            require(failure.errorCode() == -1, failure);
            require(describe().members().stream().noneMatch(m -> m.memberId().equals("c")), "failed member persisted");
            System.out.println("SCENARIO\tcallback-error-no-member\tPASS");
            select(admin, "first"); join("c"); target("first", "a"); reconcile("a");
            System.out.println("SCENARIO\trepair-retry\tPASS");
        }
    }
}
