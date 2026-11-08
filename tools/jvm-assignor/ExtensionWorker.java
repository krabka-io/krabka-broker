import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import org.apache.kafka.coordinator.group.api.streams.*;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription.*;
import org.apache.kafka.server.share.dlq.*;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import java.util.*;
import java.io.*;

/** Persistent, serial extension worker. Only trusted plugin code is supported. */
public final class ExtensionWorker {
    static final ObjectMapper JSON = new ObjectMapper();
    static Set<String> strings(JsonNode n) { var s = new LinkedHashSet<String>(); n.forEach(x -> s.add(x.textValue())); return s; }
    static Node node(JsonNode n) {
        var k = n.required("kind"); var name = n.required("name").textValue(); var next = strings(n.required("successors"));
        if (k.has("Source")) return new Source(name, strings(k.get("Source").required("topics")), next);
        if (k.has("Processor")) return new Processor(name, strings(k.get("Processor").required("stores")), next);
        if (k.has("Sink")) return new Sink(name, Optional.ofNullable(k.get("Sink").get("topic").textValue()), next);
        throw new IllegalArgumentException("unknown node kind");
    }
    static ObjectNode node(Node n) {
        var out = JSON.createObjectNode(); out.put("name", n.name()); out.set("successors", JSON.valueToTree(n.successors()));
        var kind = out.putObject("kind");
        if (n instanceof Source s) kind.putObject("Source").set("topics", JSON.valueToTree(s.topics()));
        else if (n instanceof Processor p) kind.putObject("Processor").set("stores", JSON.valueToTree(p.stores()));
        else if (n instanceof Sink s) { var sink = kind.putObject("Sink"); if (s.topic().isPresent()) sink.put("topic", s.topic().get()); else sink.putNull("topic"); }
        else throw new IllegalArgumentException("unknown Java node");
        return out;
    }
    static StreamsGroupTopologyDescription topology(JsonNode n) {
        var subs = new ArrayList<Subtopology>(); var globals = new ArrayList<GlobalStore>();
        for (var s : n.required("subtopologies")) { var nodes = new ArrayList<Node>(); s.required("nodes").forEach(x -> nodes.add(node(x))); subs.add(new Subtopology(s.required("id").textValue(), nodes)); }
        for (var g : n.required("global_stores")) globals.add(new GlobalStore((Source)node(g.required("source")), (Processor)node(g.required("processor"))));
        return new StreamsGroupTopologyDescription(subs, globals);
    }
    static JsonNode topology(StreamsGroupTopologyDescription t) {
        if (t == null) return JSON.nullNode();
        var out = JSON.createObjectNode(); var subs = out.putArray("subtopologies"); var globals = out.putArray("global_stores");
        for (var s : t.subtopologies()) { var sub = subs.addObject(); sub.put("id", s.id()); var nodes = sub.putArray("nodes"); s.nodes().forEach(n -> nodes.add(node(n))); }
        for (var g : t.globalStores()) { var store = globals.addObject(); store.set("source", node(g.source())); store.set("processor", node(g.processor())); }
        return out;
    }
    public static void main(String[] args) throws Exception {
        var output = System.out; System.setOut(System.err);
        Object plugin = Class.forName(args[1]).getConstructor().newInstance();
        var input = new BufferedReader(new InputStreamReader(System.in));
        try {
            for (String line; (line = input.readLine()) != null;) {
                var response = JSON.createObjectNode();
                try {
                    var r = JSON.readTree(line); if (r.path("version").intValue() != 1) throw new IllegalArgumentException("protocol version");
                    var op = r.required("op").textValue();
                    if (op.equals("stop")) { response.put("ok", true); output.println(response); break; }
                    if (args[0].equals("storage")) {
                        var p = (StreamsGroupTopologyDescriptionPlugin)plugin;
                        switch (op) {
                            case "configure" -> p.configure(JSON.convertValue(r.required("config"), Map.class));
                            case "set" -> p.setTopology(r.required("group").textValue(), r.required("epoch").intValue(), topology(r.required("description"))).get();
                            case "get" -> response.set("description", topology(p.getTopology(r.required("group").textValue(), r.required("epoch").intValue()).get()));
                            case "delete" -> p.deleteTopology(r.required("group").textValue()).get();
                            default -> throw new IllegalArgumentException("unknown operation");
                        }
                    } else {
                        if (op.equals("initialize")) { response.put("ok", true); output.println(response); output.flush(); continue; }
                        if (!op.equals("enqueue")) throw new IllegalArgumentException("unknown operation");
                        var uuid = java.util.UUID.fromString(r.required("topic_id").textValue());
                        var tp = new TopicIdPartition(new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()), r.required("partition").intValue(), null);
                        var cause = r.required("cause").textValue();
                        ((ShareGroupDLQManager)plugin).enqueue(new ShareGroupDLQRecordParameter(r.required("group").textValue(), tp, r.required("first").longValue(), r.required("last").longValue(), Optional.of((short)r.required("delivery_count").intValue()), Optional.of(cause.equals("rejected") ? ShareGroupDLQManager.CLIENT_REJECT : ShareGroupDLQManager.DELIVERY_COUNT_EXCEEDED))).get();
                    }
                    response.put("ok", true);
                } catch (Exception e) {
                    Throwable cause = e; while (cause.getCause() != null) cause = cause.getCause();
                    response.put("ok", false); response.put("permanent", cause instanceof StreamsTopologyDescriptionPermanentFailureException);
                }
                output.println(response); output.flush();
            }
        } finally {
            if (plugin instanceof StreamsGroupTopologyDescriptionPlugin p) p.close();
            if (plugin instanceof ShareGroupDLQManager p) p.stop();
        }
    }
}
