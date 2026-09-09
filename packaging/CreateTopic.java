import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

final class CreateTopic {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        try (Admin admin = Admin.create(properties)) {
            NewTopic topic = new NewTopic(args[1], 1, (short) 1)
                    .configs(Map.of(args[2], args[3]));
            admin.createTopics(List.of(topic)).all().get();
        }
    }
}
