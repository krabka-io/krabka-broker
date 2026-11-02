import java.nio.file.Path;
import java.util.Arrays;
import java.util.TreeSet;
import org.apache.kafka.common.record.internal.FileRecords;

/** Decode the actual uploaded prefix with released record codecs, including CRCs. */
public class CopyArchiveRecords {
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        String topic = args[1];
        int produced = Integer.parseInt(args[2]);
        TreeSet<Long> offsets = new TreeSet<>();
        byte[] expectedValue = new byte[65536];
        for (Path path : CopyProbe.files(root, topic)) {
            String name = path.getFileName().toString();
            if (!name.equals("log") && !name.endsWith(".log")) continue;
            try (FileRecords records = FileRecords.open(path.toFile(), false)) {
                for (var batch : records.batches()) {
                    batch.ensureValid();
                    if (batch.isControlBatch()) throw new IllegalStateException("unexpected control batch in fresh fixture");
                    for (var record : batch) {
                        if (!offsets.add(record.offset()) || record.offset() < 0 || record.offset() >= produced) throw new IllegalStateException("duplicate/foreign uploaded offset");
                        if (record.key() == null || record.key().remaining() != 1 || record.key().get() != (byte) record.offset()) throw new IllegalStateException("uploaded key differs from producer acknowledgement");
                        if (record.value() == null || record.value().remaining() != expectedValue.length) throw new IllegalStateException("uploaded value size changed");
                        byte[] actual = new byte[record.value().remaining()];
                        record.value().get(actual);
                        if (!Arrays.equals(actual, expectedValue)) throw new IllegalStateException("uploaded value changed");
                    }
                }
            }
        }
        if (offsets.size() < 15) throw new IllegalStateException("sealed uploaded prefix missing");
        long next = 0;
        for (long offset : offsets) if (offset != next++) throw new IllegalStateException("hole in uploaded prefix");
        System.out.println("RECORDS\t" + topic + "\tcount=" + offsets.size() + "\tCRC/key/value/contiguous-prefix-valid");
    }
}
