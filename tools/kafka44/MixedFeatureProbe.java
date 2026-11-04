import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Caller-owned three-node rolling upgrade/finalization assertions. */
public class MixedFeatureProbe {
    static void require(boolean condition, Object actual) {
        if (!condition) throw new IllegalStateException("mixed-version assertion: " + actual);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !Set.of("seed-reject", "reject", "read", "finalize", "controller", "local").contains(args[1]))
            throw new IllegalArgumentException("BOOTSTRAP MODE EXPECTED_METADATA TOPIC");
        if (args[1].equals("local")) {
            var request = new org.apache.kafka.common.requests.ApiVersionsRequest(new org.apache.kafka.common.message.ApiVersionsRequestData().setClientSoftwareName("kafka44-qualification").setClientSoftwareVersion("4.4.0"), (short) 3);
            var response = ((org.apache.kafka.common.requests.ApiVersionsResponse) ProtocolScenarios.exchange(args[0], request)).data();
            require(response.errorCode() == 0, response);
            var metadata = response.finalizedFeatures().stream().filter(f -> f.name().equals("metadata.version")).findFirst().orElseThrow();
            require(metadata.maxVersionLevel() == Short.parseShort(args[2]), response);
            System.out.println("LOCAL_FEATURES\t" + response.finalizedFeatures());
            System.out.println("SCENARIO\tlocal-finalized-metadata-level\tPASS");
            return;
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[0]);
        properties.put("default.api.timeout.ms", "30000");
        properties.put("request.timeout.ms", "10000");
        String mode = args[1], topic = args[3];
        short expected = Short.parseShort(args[2]);
        try (Admin admin = Admin.create(properties)) {
            System.out.println("CONTROLLER\t" + admin.describeCluster().controller().get());
            if (mode.equals("controller")) return;
            var nodes = admin.describeCluster().nodes().get();
            require(nodes.size() == 3, nodes);
            var before = admin.describeFeatures().featureMetadata().get();
            System.out.println("BEFORE\t" + before);
            require(before.finalizedFeatures().get("metadata.version").maxVersionLevel() == expected, before);
            System.out.println("SCENARIO\tthree-nodes-preserve-metadata-level\tPASS");
            if (!mode.equals("read")) {
                try {
                    admin.updateFeatures(Map.of("metadata.version", new FeatureUpdate((short) 33, FeatureUpdate.UpgradeType.UPGRADE)), new UpdateFeaturesOptions()).all().get();
                    require(mode.equals("finalize"), "mixed/default nodes unexpectedly allowed metadata33");
                } catch (java.util.concurrent.ExecutionException error) {
                    require(!mode.equals("finalize") && error.getCause() instanceof InvalidUpdateVersionException, error.getCause());
                    System.out.println("REJECTED\t" + error.getCause());
                }
                var after = admin.describeFeatures().featureMetadata().get();
                System.out.println("AFTER\t" + after);
                require(after.finalizedFeatures().get("metadata.version").maxVersionLevel() == (mode.equals("finalize") ? 33 : expected), after);
                System.out.println("SCENARIO\texplicit-feature-update-result\tPASS");
            }
            if (mode.equals("seed-reject")) {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3))).all().get();
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {
                    require(producer.send(new ProducerRecord<>(topic, "mixed-key", "mixed-release-record")).get().offset() == 0, "first offset");
                }
            }
            var uuid = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId();
            System.out.println("TOPIC_UUID\t" + uuid);
            properties.put("enable.auto.commit", "false");
            properties.put("group.id", topic + "-reader");
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new StringDeserializer())) {
                TopicPartition partition = new TopicPartition(topic, 0);
                consumer.assign(List.of(partition));
                consumer.seekToBeginning(List.of(partition));
                List<String> records = new ArrayList<>();
                for (int i = 0; i < 60 && records.isEmpty(); i++) {
                    for (var record : consumer.poll(Duration.ofMillis(500))) {
                        require(record.offset() == 0 && record.key().equals("mixed-key"), record);
                        records.add(record.value());
                    }
                }
                require(records.equals(List.of("mixed-release-record")), records);
                System.out.println("SCENARIO\texact-rf3-record-readable\tPASS");
            }
        }
    }
}
