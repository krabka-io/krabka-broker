import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

final class BrokerPerformanceWorkload {
    private static long percentile(long[] sorted, double percentile) {
        int index = (int) Math.ceil(percentile * sorted.length) - 1;
        return sorted[Math.max(0, index)];
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 7) {
            throw new IllegalArgumentException(
                    "bootstrap topic group records bytes records_per_second timeout_seconds");
        }
        String bootstrap = args[0];
        String topic = args[1];
        String group = args[2];
        int expected = Integer.parseInt(args[3]);
        int bytes = Integer.parseInt(args[4]);
        int rate = Integer.parseInt(args[5]);
        long timeoutNanos = Duration.ofSeconds(Long.parseLong(args[6])).toNanos();
        if (expected <= 0 || bytes < 16 || rate < -1) {
            throw new IllegalArgumentException("invalid workload bounds");
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

        long[] latencies = new long[expected];
        BitSet seen = new BitSet(expected);
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger sent = new AtomicInteger();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(
                        consumerProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(
                        producerProperties, new ByteArraySerializer(), new ByteArraySerializer())) {
            consumer.subscribe(Collections.singleton(topic));
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(100));
            }
            long started = System.nanoTime();
            CompletableFuture<Void> producing = CompletableFuture.runAsync(() -> {
                long interval = rate > 0 ? 1_000_000_000L / rate : 0;
                long next = System.nanoTime();
                for (int sequence = 0; sequence < expected; sequence++) {
                    if (interval > 0) {
                        long wait = next - System.nanoTime();
                        if (wait > 0) {
                            LockSupport.parkNanos(wait);
                        }
                        next += interval;
                    }
                    byte[] value = new byte[bytes];
                    ByteBuffer.wrap(value).putLong(System.nanoTime()).putLong(sequence);
                    producer.send(new ProducerRecord<>(topic, value), (metadata, error) -> {
                        if (error == null) {
                            sent.incrementAndGet();
                        } else {
                            errors.incrementAndGet();
                        }
                    });
                }
                producer.flush();
            });

            int consumed = 0;
            int duplicates = 0;
            long deadline = started + timeoutNanos;
            while (consumed < expected && System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(250));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    ByteBuffer value = ByteBuffer.wrap(record.value());
                    long sentAt = value.getLong();
                    long sequence = value.getLong();
                    if (sequence < 0 || sequence >= expected) {
                        throw new IllegalStateException("out-of-range sequence " + sequence);
                    }
                    int index = (int) sequence;
                    if (seen.get(index)) {
                        duplicates++;
                    } else {
                        seen.set(index);
                        latencies[consumed++] = System.nanoTime() - sentAt;
                    }
                }
            }
            producing.get();
            long elapsed = System.nanoTime() - started;
            if (errors.get() != 0 || sent.get() != expected || consumed != expected || duplicates != 0) {
                throw new IllegalStateException("sent=" + sent + " consumed=" + consumed
                        + " duplicates=" + duplicates + " errors=" + errors);
            }
            Arrays.sort(latencies);
            System.out.printf(Locale.ROOT,
                    "{\"sent\":%d,\"consumed\":%d,\"duplicates\":%d,\"errors\":%d,"
                            + "\"seconds\":%.6f,\"records_per_second\":%.3f,"
                            + "\"mib_per_second\":%.3f,\"latency_ms_p50\":%.3f,"
                            + "\"latency_ms_p95\":%.3f,\"latency_ms_p99\":%.3f}%n",
                    sent.get(), consumed, duplicates, errors.get(), elapsed / 1e9,
                    expected / (elapsed / 1e9), expected * (double) bytes / elapsed * 1e9 / 1048576,
                    percentile(latencies, 0.50) / 1e6, percentile(latencies, 0.95) / 1e6,
                    percentile(latencies, 0.99) / 1e6);
        }
    }
}
