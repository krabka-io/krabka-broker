import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

/** Caller-owned RF3 leader-movement fixture: seed delayed uploads, discover leaders, verify remote data. */
public class CopyMovementProbe {
 public static void main(String[] args) throws Exception {
  String mode=args[0], endpoint=args[1], topic=args[2];
  Properties p=new Properties(); p.put("bootstrap.servers",endpoint); p.put("default.api.timeout.ms","30000");
  if(mode.equals("leader")) {
   try(Admin a=Admin.create(p)) { var t=a.describeTopics(List.of(topic)).allTopicNames().get().get(topic); var part=t.partitions().get(0); System.out.println("LEADER\t"+part.leader().id()+"\t"+part.leader().host()+":"+part.leader().port()); } return;
  }
  if(mode.equals("seed")) {
   try(Admin a=Admin.create(p)) {
    boolean metadataReady=false;
    for(int attempt=0;attempt<120;attempt++) {
     try {
      var metadata=a.describeTopics(List.of("__remote_log_metadata")).allTopicNames().get().get("__remote_log_metadata");
      metadataReady=metadata.partitions().stream().allMatch(part -> part.leader()!=null && part.replicas().size()==3 && part.isr().size()==3);
      if(metadataReady) break;
     } catch(java.util.concurrent.ExecutionException e) {
      if(!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) throw e;
     }
     Thread.sleep(500);
    }
    if(!metadataReady) throw new IllegalStateException("RF3 remote metadata topic not ready before fault");
    System.out.println("PRECONDITION\tremote-metadata-rf3-isr3-ready");
    a.createTopics(List.of(new NewTopic(topic,1,(short)3).configs(Map.of(
    "remote.storage.enable","true","segment.bytes","1048576","max.message.bytes","131072",
    "remote.copy.lag.ms","30000","remote.copy.lag.bytes","100000000",
    "local.retention.ms","35000","local.retention.bytes","-1")))).all().get(); }
   p.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer"); p.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer"); p.put("acks","all");
   try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(p)) {
    for(int n=0;n<48;n++) {
     byte[] value=new byte[65536]; Arrays.fill(value,(byte)n);
     long offset=producer.send(new ProducerRecord<>(topic,new byte[]{(byte)n},value)).get().offset();
     if(offset!=n) throw new IllegalStateException("acknowledged offset mismatch "+offset);
    }
   }
   if(!CopyProbe.files(Path.of(args[3]),topic).isEmpty()) throw new IllegalStateException("unexpected early upload");
   System.out.println("SCENARIO\tdelayed-copy-seeded-rf3-48-before-movement\tPASS"); return;
  }
  if(!mode.equals("read")) throw new IllegalArgumentException("seed, leader or read");
  if(args.length!=5 || Files.exists(Path.of(args[3])) || CopyProbe.files(Path.of(args[4]),topic).isEmpty())
   throw new IllegalStateException("read requires physically evicted local offset0 and topic archive");
  System.out.println("WITNESS\tlocal-offset0-absent\t"+args[3]);
  p.put("key.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer"); p.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer"); p.put("enable.auto.commit","false"); p.put("auto.offset.reset","none");
  try(KafkaConsumer<byte[],byte[]> c=new KafkaConsumer<>(p)) {
   TopicPartition tp=new TopicPartition(topic,0); c.assign(List.of(tp)); c.seek(tp,0);
   int count=0; long deadline=System.nanoTime()+Duration.ofSeconds(90).toNanos();
   while(count<48 && System.nanoTime()<deadline) for(ConsumerRecord<byte[],byte[]> r:c.poll(Duration.ofSeconds(1))) {
    if(r.offset()!=count || !Arrays.equals(r.key(),new byte[]{(byte)count}) || r.value()==null || r.value().length!=65536) throw new IllegalStateException("record identity mismatch "+count);
    for(byte b:r.value()) if(b!=(byte)count) throw new IllegalStateException("payload mismatch "+count);
    count++;
   }
   if(count!=48) throw new IllegalStateException("missing records "+count);
   System.out.println("SCENARIO\tpost-movement-eviction-read-exact-48\tPASS");
  }
 }
}
