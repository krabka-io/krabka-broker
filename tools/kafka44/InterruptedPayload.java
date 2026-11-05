import java.nio.file.Path;
import org.apache.kafka.common.record.internal.FileRecords;

/** Released-codec validation of the actual gated offset-zero payload file. */
public class InterruptedPayload {
 public static void main(String[] args) throws Exception {
  if(args.length!=1) throw new IllegalArgumentException("expected gated payload path");
  int count=0;
  try(FileRecords records=FileRecords.open(Path.of(args[0]).toFile(),false)) {
   for(var batch:records.batches()) {
    batch.ensureValid();
    if(batch.isControlBatch()) throw new IllegalStateException("control batch");
    for(var record:batch) {
     if(record.offset()!=count || record.key()==null || record.key().remaining()!=1 || record.key().get()!=(byte)count || record.value()==null || record.value().remaining()!=65536)
      throw new IllegalStateException("record identity "+count);
     var value=record.value();while(value.hasRemaining()) if(value.get()!=(byte)count) throw new IllegalStateException("payload "+count);
     count++;
    }
   }
  }
  if(count!=15) throw new IllegalStateException("gated prefix count "+count);
  System.out.println("SCENARIO\tgated-real-payload-15-exact-records-valid-CRC\tPASS");
 }
}
