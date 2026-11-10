import org.apache.kafka.coordinator.group.api.streams.assignor.*;
import java.util.*;
public class Fixture implements TaskAssignor, AutoCloseable {
    public void close() { if ("closefail".equals(System.getenv("JVM_FIXTURE_MODE"))) throw new IllegalStateException("close failed"); }
    public String name() { return "fixture"; }
    public GroupAssignment assign(GroupSpec group, TopologyDescriber topology) {
        String mode = System.getenv("JVM_FIXTURE_MODE");
        if ("throw".equals(mode)) throw new IllegalStateException("fixture failure");
        if ("exit".equals(mode)) System.exit(19);
        if ("hang".equals(mode)) { while (true) { try { Thread.sleep(1000); } catch (InterruptedException e) {} } }
        if ("flood".equals(mode)) { while (true) System.out.println("x".repeat(8192)); }
        List<String> ids = new ArrayList<>(group.memberIds());
        String id = "last".equals(mode) ? ids.getLast() : ids.getFirst();
        if ("unknown".equals(mode)) id = "unknown-owner";
        Map<String, Set<Integer>> tasks = new TreeMap<>();
        for (String sub : topology.subtopologies()) {
            Set<Integer> ps = new TreeSet<>();
            for (int p = 0; p < topology.maxNumInputPartitions(sub); p++) ps.add(p);
            tasks.put(sub, ps);
        }
        // Exercise every converted accessor, including nested offset values.
        for (String member : ids) {
            var metadata = group.memberMetadata(member);
            metadata.instanceId(); metadata.rackId(); metadata.processId(); metadata.clientTags();
            var state = group.memberAssignmentState(member);
            state.activeTasks(); state.standbyTasks(); state.warmupTasks(); state.taskOffsets(); state.taskEndOffsets();
            if (member.equals("a") && state.taskOffsets().get("sub").get(0) != Long.MAX_VALUE) throw new AssertionError("offset conversion lost precision");
            if (!metadata.processId().equals(member.equals("a") ? "p1" : "p2")) throw new AssertionError("process ID conversion");
        }
        group.configs().numStandbyReplicas(); group.configs().rackAwareAssignmentTags();
        System.out.println("plugin stdout does not corrupt protocol");
        return new GroupAssignment(Map.of(id, new MemberAssignment(tasks, Map.of())));
    }
}
