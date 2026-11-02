import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Owned POSIX archive failure/retry smoke; restores original permissions through finally (task also restores after timeout). */
public class FaultCopyProbe {
    public static void main(String[] args) throws Exception {
        String topic = "copy-fault-" + java.util.UUID.randomUUID();
        Path archive = Path.of(args[1]);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", args[0]);
        properties.put("default.api.timeout.ms", "30000");
        Set<PosixFilePermission> original = Files.getPosixFilePermissions(archive);
        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1).configs(Map.of(
                "remote.storage.enable", "true", "segment.bytes", "1048576", "max.message.bytes", "131072",
                "remote.copy.lag.ms", "0", "remote.copy.lag.bytes", "100000000")))).all().get();
            Files.setPosixFilePermissions(archive, Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_EXECUTE,
                PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_EXECUTE));
            if (Files.isWritable(archive)) throw new IllegalStateException("fault did not deny this process archive writes");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
            properties.put("acks", "all");
            properties.put("max.block.ms", "10000");
            properties.put("request.timeout.ms", "10000");
            properties.put("delivery.timeout.ms", "30000");
            properties.put("linger.ms", "0");
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
                for (int n = 0; n < 24; n++) producer.send(new ProducerRecord<>(topic, new byte[]{(byte) n}, new byte[65536])).get();
            }
            Thread.sleep(3000);
            if (!CopyProbe.files(archive, topic).isEmpty()) throw new IllegalStateException("archive write succeeded during denied access");
            System.out.println("FAULT\t" + topic + "\tarchive-writes-denied-for-3s");
            Files.setPosixFilePermissions(archive, original);
            for (int attempt = 0; attempt < 120; attempt++) {
                if (!CopyProbe.files(archive, topic).isEmpty()) {
                    System.out.println("DETAIL\t" + topic + "\tarchive-present-after-permission-recovery");
                    System.out.println("SCENARIO\tdelay-then-failed-upload-copies\tPASS");
                    return;
                }
                Thread.sleep(500);
            }
            throw new IllegalStateException("archive did not retry after access recovered");
        } finally {
            Files.setPosixFilePermissions(archive, original);
        }
    }
}
