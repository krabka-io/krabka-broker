import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.record.internal.*;

/** Real share-consumer rejection and DLQ source-copy probe. Fresh names required. */
public class ShareBoundary {
  static Properties props(String bootstrap) {
    Properties p=new Properties(); p.put("bootstrap.servers",bootstrap);
    p.put("request.timeout.ms","10000"); p.put("default.api.timeout.ms","30000");
    return p;
  }
  public static void main(String[] args) throws Exception {
    String bootstrap=args[0], run=args[1];
    try(Admin admin=Admin.create(props(bootstrap))) {
      if(args.length>2 && args[2].equals("verify")) {
        for(boolean c:new boolean[]{true,false}) for(int i=0;i<4;i++) {
          String suffix=run+(c?"t":"f")+i, group="grp"+suffix;
          var tp=new org.apache.kafka.common.TopicPartition("src"+suffix,0);
          var info=admin.listShareGroupOffsets(Map.of(group,new ListShareGroupOffsetsSpec().topicPartitions(List.of(tp)))).partitionsToOffsetInfo(group).get().get(tp);
          if(info==null || !info.lag().equals(Optional.of(0L)))throw new AssertionError("nonzero lag: "+info);
          System.out.printf("group=%s source_start_offset=%d lag=%d%n",group,info.startOffset(),info.lag().orElse(-1L));
          System.out.printf("SCENARIO\tdlq-recovery-%s-%d\tPASS%n",c,i);
        }
        return;
      }
      for(boolean copy:new boolean[]{true,false}) {
        int size=0;
        for(int step=0;step<4;step++) {
          String suffix=run+(copy?"t":"f")+step;
          String src="src"+suffix, dst="dlq."+suffix, group="grp"+suffix;
          int limit=step==0?100000:size+step-2;
          admin.createTopics(List.of(new NewTopic(src,1,(short)1),new NewTopic(dst,1,(short)1).configs(Map.of("errors.deadletterqueue.group.enable","true","max.message.bytes",""+limit)))).all().get();
          ConfigResource resource=new ConfigResource(ConfigResource.Type.GROUP,group);
          List<AlterConfigOp> ops=new ArrayList<>();
          for(var e:Map.of("errors.deadletterqueue.topic.name",dst,"errors.deadletterqueue.copy.record.enable",""+copy,"share.auto.offset.reset","earliest").entrySet()) ops.add(new AlterConfigOp(new ConfigEntry(e.getKey(),e.getValue()),AlterConfigOp.OpType.SET));
          admin.incrementalAlterConfigs(Map.of(resource,ops)).all().get();
          Properties p=props(bootstrap);p.put("key.serializer",ByteArraySerializer.class.getName());p.put("value.serializer",ByteArraySerializer.class.getName());
          try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(p)){producer.send(new ProducerRecord<>(src,new byte[]{1},new byte[100])).get();}
          p=props(bootstrap); p.put("key.deserializer",ByteArrayDeserializer.class.getName());p.put("value.deserializer",ByteArrayDeserializer.class.getName());p.put("group.id",group);p.put("share.acknowledgement.mode","explicit");
          try(KafkaShareConsumer<byte[],byte[]> consumer=new KafkaShareConsumer<>(p)) {
            consumer.subscribe(List.of(src)); boolean rejected=false; long end=System.nanoTime()+Duration.ofSeconds(35).toNanos();
            while(!rejected && System.nanoTime()<end) for(var record:consumer.poll(Duration.ofSeconds(1))) {consumer.acknowledge(record,AcknowledgeType.REJECT);rejected=true;}
            if(!rejected)throw new AssertionError("no source record "+src);
            var results=consumer.commitSync();for(var e:results.values())if(e.isPresent())throw e.get();
          }
          p=props(bootstrap);p.put("key.deserializer",ByteArrayDeserializer.class.getName());p.put("value.deserializer",ByteArrayDeserializer.class.getName());p.put("group.id","verify"+suffix);p.put("auto.offset.reset","earliest");
          int count=0, value=-1, bytes=0;
          try(KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(p)) {
            consumer.assign(List.of(new org.apache.kafka.common.TopicPartition(dst,0)));consumer.seekToBeginning(consumer.assignment());
            long end=System.nanoTime()+Duration.ofSeconds(step==1?4:12).toNanos();
            while(count==0 && System.nanoTime()<end) for(var record:consumer.poll(Duration.ofMillis(200))) {
              count++;value=record.value()==null?-1:record.value().length;
              if(!Arrays.equals(record.key(),copy?new byte[]{1}:null) || !Arrays.equals(record.value(),copy?new byte[100]:null))throw new AssertionError("incorrect DLQ payload");
              var expectedHeaders=Map.of("__dlq.errors.topic",src,"__dlq.errors.partition","0","__dlq.errors.offset","0","__dlq.errors.group",group,"__dlq.errors.delivery.count","1","__dlq.errors.message","Offset rejected by client.");
              if(record.headers().toArray().length!=expectedHeaders.size())throw new AssertionError("incorrect provenance header count");
              for(var e:expectedHeaders.entrySet()) {
                var h=record.headers().lastHeader(e.getKey());
                if(h==null || !Arrays.equals(h.value(),e.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8)))throw new AssertionError("incorrect provenance header "+e.getKey());
              }
              bytes=MemoryRecords.withRecords(org.apache.kafka.common.compress.Compression.NONE,new SimpleRecord(record.timestamp(),record.key(),record.value(),record.headers().toArray())).sizeInBytes();
            }
          }
          if(step==0)size=bytes;
          int expected=step==1?0:1;
          if(count!=expected || (count>0 && value!=(copy?100:-1)))throw new AssertionError("count="+count+" value="+value+" limit="+limit+" size="+size);
          System.out.printf("copy=%s limit=%d encoded_bytes=%d dlq_count=%d value_bytes=%d%n",copy,limit,size,count,value);
          System.out.printf("SCENARIO\tdlq-boundary-%s-%d\tPASS%n",copy,step);
        }
      }
    }
  }
}
