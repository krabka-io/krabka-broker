import java.util.*;
import org.apache.kafka.coordinator.group.api.streams.assignor.*;

/** Deterministic equivalent of the native fixture, not a production assignor. */
public class AssignorPlugin implements TaskAssignor {
    public String name() { return "first"; }
    protected boolean reverse() { return false; }
    public GroupAssignment assign(GroupSpec spec, TopologyDescriber topology) {
        var ids = new ArrayList<>(spec.memberIds());
        Collections.sort(ids);
        String owner = ids.get(reverse() ? ids.size() - 1 : 0);
        Map<String, Set<Integer>> tasks = new TreeMap<>();
        for (String sub : topology.subtopologies()) {
            Set<Integer> parts = new TreeSet<>();
            for (int i = 0; i < topology.maxNumInputPartitions(sub); i++) parts.add(i);
            tasks.put(sub, parts);
        }
        System.out.println("ASSIGNOR_CALLBACK " + name() + " " + owner + " " + tasks);
        return new GroupAssignment(Map.of(owner, new MemberAssignment(tasks, Map.of())));
    }
    public static class Last extends AssignorPlugin {
        public String name() { return "last"; }
        protected boolean reverse() { return true; }
    }
    public static class Failure extends AssignorPlugin {
        public String name() { return "failure"; }
        public GroupAssignment assign(GroupSpec spec, TopologyDescriber topology) {
            throw new TaskAssignorException("injected assignor failure");
        }
    }
    /** Deliberately invalid output: task owner is not in GroupSpec.memberIds(). */
    public static class UnknownMember extends AssignorPlugin {
        public String name() { return "unknown-member"; }
        public GroupAssignment assign(GroupSpec spec, TopologyDescriber topology) {
            System.out.println("ASSIGNOR_CALLBACK unknown-member injected-unknown-member");
            return new GroupAssignment(Map.of("injected-unknown-member",
                new MemberAssignment(Map.of("0", Set.of(0, 1)), Map.of())));
        }
    }

}
