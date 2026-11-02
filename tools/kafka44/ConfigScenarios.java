import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.protocol.Errors;

/** Released-client config discovery and validate-only delayed-copy boundaries. */
public class ConfigScenarios {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("isolated broker endpoint required");
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[0]);
        properties.put("default.api.timeout.ms", "30000");
        String topic = "kafka44-config-" + java.util.UUID.randomUUID();
        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            var original = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            for (String key : List.of("remote.copy.lag.ms", "remote.copy.lag.bytes")) {
                ConfigEntry entry = original.get(key);
                System.out.println("SCENARIO\tdiscover-" + key + "\t" + (entry == null ? "MISSING" : entry.value()));
                for (String value : List.of("-2", "-1", "0", "1000", "not-a-number")) {
                    String result;
                    try {
                        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(
                            new ConfigEntry(key, value), AlterConfigOp.OpType.SET))),
                            new AlterConfigsOptions().validateOnly(true)).all().get();
                        result = Errors.NONE.name();
                    } catch (ExecutionException error) {
                        result = Errors.forException(error.getCause()).name();
                        System.out.println("DETAIL\t" + key + "=" + value + "\t" + error.getCause());
                    }
                    System.out.println("SCENARIO\tvalidate-" + key + "=" + value + "\t" + result);
                }
            }
            var unchanged = admin.describeConfigs(List.of(resource)).all().get().get(resource);
            for (String key : List.of("remote.copy.lag.ms", "remote.copy.lag.bytes")) {
                if (!java.util.Objects.equals(original.get(key), unchanged.get(key))) {
                    throw new IllegalStateException("validate-only changed " + key);
                }
            }
            System.out.println("SCENARIO\tvalidate-only-preserves-config\tPASS");
        }
    }
}
