import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;

/** Real discovery and validate-only DLQ configuration boundaries on owned fixtures. */
public class DlqConfigScenarios {
    record Boundary(String key, List<String> values) {}
    static void inspect(Admin admin, ConfigResource resource, List<Boundary> boundaries) throws Exception {
        var original = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        for (var boundary : boundaries) {
            var discovered = original.get(boundary.key());
            if (discovered == null) throw new IllegalStateException("missing " + boundary.key());
            String expectedDefault = boundary.key().endsWith(".enable") ? "false" : "";
            if (!Objects.equals(discovered.value(), expectedDefault))
                throw new IllegalStateException("unexpected default " + discovered);
            System.out.println("SCENARIO\tdiscover-" + boundary.key() + "\t" + discovered.value());
            for (var value : boundary.values()) {
                String result = Errors.NONE.name();
                try {
                    admin.incrementalAlterConfigs(Map.of(resource,List.of(new AlterConfigOp(
                        new ConfigEntry(boundary.key(),value),AlterConfigOp.OpType.SET))),
                        new AlterConfigsOptions().validateOnly(true)).all().get();
                } catch (ExecutionException error) {
                    result = Errors.forException(error.getCause()).name();
                    System.out.println("DETAIL\t" + boundary.key() + "=" + value + "\t" + error.getCause());
                }
                String expected = boundary.key().endsWith(".enable")
                    ? (List.of("true","false").contains(value) ? "NONE" : "INVALID_CONFIG")
                    : (value.startsWith("__") ? "INVALID_CONFIG" : "NONE");
                if (!result.equals(expected)) throw new IllegalStateException(boundary.key() + "=" + value + ": " + result);
                System.out.println("SCENARIO\tvalidate-" + boundary.key() + "=" + value + "\t" + result);
            }
        }
        var after = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        for (var boundary : boundaries)
            if (!Objects.equals(original.get(boundary.key()),after.get(boundary.key())))
                throw new IllegalStateException("validate-only mutated " + boundary.key());
        System.out.println("SCENARIO\tvalidate-only-preserves-" + resource.type() + "\tPASS");
    }
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("ISOLATED_ENDPOINT");
        var properties = new Properties();
        properties.put("bootstrap.servers",args[0]);
        properties.put("default.api.timeout.ms","30000");
        String name = "dlq-config-" + UUID.randomUUID();
        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(List.of(new NewTopic(name,1,(short)1))).all().get();
            inspect(admin,new ConfigResource(ConfigResource.Type.TOPIC,name),List.of(
                new Boundary("errors.deadletterqueue.group.enable",List.of("false","true","yes","1",""))));
            inspect(admin,new ConfigResource(ConfigResource.Type.GROUP,name),List.of(
                new Boundary("errors.deadletterqueue.copy.record.enable",List.of("false","true","yes","1","")),
                new Boundary("errors.deadletterqueue.topic.name",List.of("","orders-dlq","__internal","bad name",".."))));
        }
    }
}
