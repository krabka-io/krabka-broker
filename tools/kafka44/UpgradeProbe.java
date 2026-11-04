import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/** Released-codec assertions for a caller-owned isolated upgrade data directory. */
public class UpgradeProbe {
    static void require(boolean condition, Object actual) {
        if (!condition) throw new IllegalStateException("upgrade assertion: " + actual);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || !Set.of("seed", "upgrade", "rollback").contains(args[1])
                || args.length != (args[1].equals("seed") ? 3 : 5))
            throw new IllegalArgumentException("ENDPOINT seed|upgrade|rollback TOPIC [UUID FEATURES]");
        String endpoint = args[0], mode = args[1], topic = args[2], group = topic + "-group";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", endpoint);
        properties.put("default.api.timeout.ms", "30000");
        properties.put("request.timeout.ms", "10000");
        TopicPartition partition = new TopicPartition(topic, 0);
        try (Admin admin = Admin.create(properties)) {
            if (mode.equals("seed")) admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
            String uuid = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic).topicId().toString();
            var features = new TreeMap<>(admin.describeFeatures().featureMetadata().get().finalizedFeatures()).toString();
            System.out.println("IDENTITY\t" + uuid + "\t" + features);
            if (!mode.equals("seed")) {
                require(uuid.equals(args[3]), uuid);
                require(features.equals(args[4]), features);
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
                require(offsets.containsKey(partition) && offsets.get(partition).offset() == 1L, offsets);
                System.out.println("SCENARIO\tidentity-features-offset-preserved\tPASS");
            }
            if (mode.equals("upgrade")) {
                try {
                    admin.updateFeatures(Map.of("metadata.version", new FeatureUpdate((short) 33, FeatureUpdate.UpgradeType.UPGRADE)), new UpdateFeaturesOptions()).all().get();
                    throw new IllegalStateException("unqualified metadata33 unexpectedly finalized without unstable gates");
                } catch (java.util.concurrent.ExecutionException error) {
                    require(error.getCause() instanceof InvalidUpdateVersionException, error.getCause());
                }
                require(new TreeMap<>(admin.describeFeatures().featureMetadata().get().finalizedFeatures()).toString().equals(features), "rejected update changed persisted levels");
                System.out.println("SCENARIO\tunsupported-finalization-refused-without-mutation\tPASS");
            }
            if (mode.equals("seed") || mode.equals("upgrade") || mode.equals("rollback")) {
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {
                    var result = producer.send(new ProducerRecord<>(topic, "key", mode.equals("seed") ? "previous-release" : mode.equals("upgrade") ? "candidate-write" : "rollback-write")).get();
                    require(result.offset() == (mode.equals("seed") ? 0 : mode.equals("upgrade") ? 1 : 2), result);
                }
            }
            Properties consumerProperties = new Properties();
            consumerProperties.putAll(properties);
            consumerProperties.put("group.id", group);
            consumerProperties.put("enable.auto.commit", "false");
            consumerProperties.put("auto.offset.reset", "earliest");
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer())) {
                consumer.assign(List.of(partition));
                consumer.seekToBeginning(List.of(partition));
                List<String> values = new ArrayList<>();
                int expected = mode.equals("seed") ? 1 : mode.equals("upgrade") ? 2 : 3;
                for (int attempt = 0; attempt < 60 && values.size() < expected; attempt++) {
                    for (var record : consumer.poll(Duration.ofMillis(500))) {
                        require(record.key().equals("key") && record.offset() == values.size(), record);
                        values.add(record.value());
                    }
                }
                require(values.equals(expected == 1 ? List.of("previous-release") : expected == 2 ? List.of("previous-release", "candidate-write") : List.of("previous-release", "candidate-write", "rollback-write")), values);
                if (mode.equals("seed")) consumer.commitSync(Map.of(partition, new OffsetAndMetadata(1L)));
                System.out.println("SCENARIO\texact-records-readable\tPASS");
            }
        }
    }
}
