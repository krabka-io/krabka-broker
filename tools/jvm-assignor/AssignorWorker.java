import com.fasterxml.jackson.databind.*;
import org.apache.kafka.coordinator.group.api.streams.assignor.*;
import java.util.*;

/** Single-request worker. Protocol lives on stdout; plugin prints go to stderr. */
public final class AssignorWorker {
    static final ObjectMapper JSON = new ObjectMapper();
    static Map<String, Set<Integer>> tasks(JsonNode n) {
        Map<String, Set<Integer>> result = new TreeMap<>();
        n.fields().forEachRemaining(e -> {
            Set<Integer> partitions = new TreeSet<>();
            e.getValue().forEach(p -> partitions.add(p.intValue()));
            result.put(e.getKey(), partitions);
        });
        return result;
    }
    static Map<String, Set<Integer>> canonical(Map<String, Set<Integer>> source) {
        Map<String, Set<Integer>> result = new TreeMap<>();
        source.forEach((sub, ps) -> result.put(sub, new TreeSet<>(ps)));
        return result;
    }
    static Map<String, Map<Integer, Long>> offsets(JsonNode n) {
        Map<String, Map<Integer, Long>> result = new TreeMap<>();
        n.fields().forEachRemaining(e -> {
            Map<Integer, Long> partitions = new TreeMap<>();
            e.getValue().fields().forEachRemaining(p -> partitions.put(Integer.parseInt(p.getKey()), p.getValue().longValue()));
            result.put(e.getKey(), partitions);
        });
        return result;
    }
    public static void main(String[] args) throws Exception {
        var output = System.out;
        System.setOut(System.err);
        JsonNode request = JSON.readTree(System.in);
        if (request.path("version").intValue() != 1) throw new IllegalArgumentException("unsupported protocol version");
        Map<String, JsonNode> members = new TreeMap<>();
        request.path("members").forEach(m -> {
            String id = m.required("member_id").textValue();
            if (members.put(id, m) != null) throw new IllegalArgumentException("duplicate member");
        });
        JsonNode input = request.required("input");
        Map<String, Set<Integer>> universe = tasks(input.required("tasks"));
        // Kafka describes task universes as contiguous [0, partition count).
        universe.forEach((sub, ps) -> { int i = 0; for (int p : ps) if (p != i++) throw new IllegalArgumentException("noncontiguous task universe"); });
        Set<String> stateful = new HashSet<>();
        input.path("stateful").forEach(s -> stateful.add(s.textValue()));
        GroupSpec group = new GroupSpec() {
            public Collection<String> memberIds() { return members.keySet(); }
            public MemberAssignmentMetadata memberMetadata(String id) {
                JsonNode m = Objects.requireNonNull(members.get(id));
                return new MemberAssignmentMetadata() {
                    public Optional<String> instanceId() { return Optional.empty(); }
                    public Optional<String> rackId() { return Optional.empty(); }
                    public String processId() { return m.required("process_id").textValue(); }
                    public Map<String, String> clientTags() { return Map.of(); }
                };
            }
            public MemberAssignmentState memberAssignmentState(String id) {
                JsonNode m = Objects.requireNonNull(members.get(id));
                return new MemberAssignmentState() {
                    public Map<String, Set<Integer>> activeTasks() { return tasks(m.path("current_active")); }
                    public Map<String, Set<Integer>> standbyTasks() { return tasks(m.path("current_standby")); }
                    public Map<String, Set<Integer>> warmupTasks() { return tasks(m.path("current_warmup")); }
                    public Map<String, Map<Integer, Long>> taskOffsets() { return offsets(m.path("task_offsets")); }
                    public Map<String, Map<Integer, Long>> taskEndOffsets() { return Map.of(); }
                };
            }
            public AssignmentConfigs configs() { return new AssignmentConfigs() {
                public int numStandbyReplicas() { return input.path("num_standby_replicas").intValue(); }
                public List<String> rackAwareAssignmentTags() { return List.of(); }
            }; }
        };
        TopologyDescriber topology = new TopologyDescriber() {
            public List<String> subtopologies() { return new ArrayList<>(universe.keySet()); }
            public int maxNumInputPartitions(String sub) { return Objects.requireNonNull(universe.get(sub)).size(); }
            public boolean isStateful(String sub) { return stateful.contains(sub); }
        };
        TaskAssignor plugin = (TaskAssignor) Class.forName(args[0]).getDeclaredConstructor().newInstance();
        try {
            GroupAssignment assignment = plugin.assign(group, topology);
            Map<String, Object> active = new TreeMap<>(), standby = new TreeMap<>();
            assignment.members().forEach((id, a) -> { active.put(id, canonical(a.activeTasks())); standby.put(id, canonical(a.standbyTasks())); });
            output.println(JSON.writeValueAsString(Map.of("version", 1, "name", plugin.name(), "active", active, "standby", standby)));
        } finally {
            if (plugin instanceof AutoCloseable closeable) closeable.close();
        }
    }
}
