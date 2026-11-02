import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.*;

/** Three-node source restart while a destination leader is paused. */
public class ShareFault {
  static final String RUN=System.getProperty("dlq.run","fault02");
  static final String SOURCE="source-"+RUN, DEST="dlq."+RUN, GROUP="group-"+RUN;
  public static void main(String[] args) throws Exception {
    var p=ShareBoundary.props(args[0]);
    if(args[1].equals("setup")) {
      try(Admin admin=Admin.create(p)) {
        admin.createTopics(List.of(new NewTopic(SOURCE,Map.of(0,List.of(1))),new NewTopic(DEST,Map.of(0,List.of(2,3))).configs(Map.of("errors.deadletterqueue.group.enable","true","min.insync.replicas","1")))).all().get();
        var ops=new ArrayList<AlterConfigOp>();
        for(var e:Map.of("errors.deadletterqueue.topic.name",DEST,"errors.deadletterqueue.copy.record.enable","true","share.auto.offset.reset","earliest").entrySet())ops.add(new AlterConfigOp(new ConfigEntry(e.getKey(),e.getValue()),AlterConfigOp.OpType.SET));
        admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP,GROUP),ops)).all().get();
      }
      p.put("key.serializer",ByteArraySerializer.class.getName());p.put("value.serializer",ByteArraySerializer.class.getName());
      try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(p)){producer.send(new ProducerRecord<>(SOURCE,new byte[]{1},new byte[100])).get();}
      try(Admin admin=Admin.create(ShareBoundary.props(args[0]))) {
        var info=admin.describeTopics(List.of(SOURCE,DEST)).allTopicNames().get();
        if(info.get(SOURCE).partitions().get(0).leader().id()!=1 || info.get(DEST).partitions().get(0).leader().id()!=2)throw new AssertionError("unexpected leaders "+info);
      }
      System.out.println("SETUP_COMPLETE source_leader=1 destination_leader=2 state_leader=3");
      return;
    }
    p.put("key.deserializer",ByteArrayDeserializer.class.getName());p.put("value.deserializer",ByteArrayDeserializer.class.getName());
    if(args[1].equals("resume")) {
      p.put("group.id",GROUP);p.put("share.acknowledgement.mode","explicit");
      try(KafkaShareConsumer<byte[],byte[]> consumer=new KafkaShareConsumer<>(p)) {
        consumer.subscribe(List.of(SOURCE));long end=System.nanoTime()+Duration.ofSeconds(15).toNanos();
        while(System.nanoTime()<end)if(!consumer.poll(Duration.ofMillis(250)).isEmpty())throw new AssertionError("rejected source redelivered");
      }
      System.out.println("RECOVERY_POLL_EMPTY");return;
    }
    if(args[1].equals("reject")) {
      p.put("group.id",GROUP);p.put("share.acknowledgement.mode","explicit");
      try(KafkaShareConsumer<byte[],byte[]> consumer=new KafkaShareConsumer<>(p)) {
        consumer.subscribe(List.of(SOURCE));long end=System.nanoTime()+Duration.ofSeconds(60).toNanos();
        while(System.nanoTime()<end) for(var record:consumer.poll(Duration.ofMillis(500))) {
          System.out.println("SOURCE_ACQUIRED offset="+record.offset());System.out.flush();
          while(!Files.exists(Path.of(args[2])))Thread.sleep(25);
          consumer.acknowledge(record,AcknowledgeType.REJECT);
          System.out.println("REJECT_COMMIT_START");System.out.flush();
          System.out.println("REJECT_COMMIT_RESULT "+consumer.commitSync(Duration.ofSeconds(60)));
          return;
        }
        throw new AssertionError("no source record");
      }
    }
    p.put("group.id","fault-verifier");p.put("auto.offset.reset","earliest");int count=0;
    try(KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(p)) {
      consumer.assign(List.of(new org.apache.kafka.common.TopicPartition(DEST,0)));consumer.seekToBeginning(consumer.assignment());
      long end=System.nanoTime()+Duration.ofSeconds(15).toNanos();
      while(System.nanoTime()<end)for(var r:consumer.poll(Duration.ofMillis(250))) {count++;if(r.value()==null||r.value().length!=100)throw new AssertionError("lost payload");}
    }
    try(Admin admin=Admin.create(ShareBoundary.props(args[0]))) {
      var tp=new org.apache.kafka.common.TopicPartition(SOURCE,0);
      var info=admin.listShareGroupOffsets(Map.of(GROUP,new ListShareGroupOffsetsSpec().topicPartitions(List.of(tp)))).partitionsToOffsetInfo(GROUP).get().get(tp);
      if(!info.lag().equals(Optional.of(0L)))throw new AssertionError("not terminal: "+info);
      if(count<1)throw new AssertionError("lost DLQ record");
      System.out.printf("FINAL dlq_records=%d start=%d lag=%d%n",count,info.startOffset(),info.lag().get());
    }
  }
}
