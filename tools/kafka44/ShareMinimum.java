import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Caller-owned fresh brokers only: level1 reject, upgrade, level2 reject. */
public class ShareMinimum {
 static Properties props(String endpoint) { Properties p=new Properties();p.put("bootstrap.servers",endpoint);p.put("default.api.timeout.ms","30000");p.put("request.timeout.ms","10000");return p; }
 static void require(boolean ok,Object detail) { if(!ok)throw new AssertionError(detail); }
 static void pass(String name) { System.out.println("SCENARIO\t"+name+"\tPASS"); }
 public static void main(String[] args)throws Exception {
  String endpoint=args[0];boolean defaults=args[1].equals("default");
  try(Admin admin=Admin.create(props(endpoint))) {
   var features=admin.describeFeatures().featureMetadata().get();System.out.println("FEATURES\tbefore\t"+features);
   require(features.finalizedFeatures().get("share.version").maxVersionLevel()==1,"requires fresh level1");
   if(defaults) {
    try {admin.updateFeatures(Map.of("share.version",new FeatureUpdate((short)2,FeatureUpdate.UpgradeType.UPGRADE)),new UpdateFeaturesOptions()).all().get();throw new AssertionError("default unexpectedly supports level2");}
    catch(java.util.concurrent.ExecutionException e) {require(e.getCause() instanceof org.apache.kafka.common.errors.InvalidUpdateVersionException,e);System.out.println("RESPONSE\tdefault-update\t"+e.getCause());}
    require(admin.describeFeatures().featureMetadata().get().finalizedFeatures().get("share.version").maxVersionLevel()==1,"failed update changed level");pass("default-refuses-share2");return;
   }
   for(int level=1;level<=2;level++) {
    if(level==2)admin.updateFeatures(Map.of("share.version",new FeatureUpdate((short)2,FeatureUpdate.UpgradeType.UPGRADE)),new UpdateFeaturesOptions()).all().get();
    System.out.println("FEATURES\tlevel"+level+"\t"+admin.describeFeatures().featureMetadata().get());
    String suffix=UUID.randomUUID().toString(),src="src-"+suffix,dst="dlq."+suffix,group="grp-"+suffix;
    admin.createTopics(List.of(new NewTopic(src,1,(short)1),new NewTopic(dst,1,(short)1).configs(Map.of("errors.deadletterqueue.group.enable","true")))).all().get();
    List<AlterConfigOp> ops=new ArrayList<>();for(var e:Map.of("errors.deadletterqueue.topic.name",dst,"errors.deadletterqueue.copy.record.enable","true","share.auto.offset.reset","earliest").entrySet())ops.add(new AlterConfigOp(new ConfigEntry(e.getKey(),e.getValue()),AlterConfigOp.OpType.SET));
    admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP,group),ops)).all().get();
    Properties p=props(endpoint);p.put("key.serializer",ByteArraySerializer.class.getName());p.put("value.serializer",ByteArraySerializer.class.getName());
    try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(p)){require(producer.send(new ProducerRecord<>(src,new byte[]{1},new byte[]{2,3})).get().offset()==0,"source offset");}
    p=props(endpoint);p.put("key.deserializer",ByteArrayDeserializer.class.getName());p.put("value.deserializer",ByteArrayDeserializer.class.getName());p.put("group.id",group);p.put("share.acknowledgement.mode","explicit");
    try(KafkaShareConsumer<byte[],byte[]> consumer=new KafkaShareConsumer<>(p)) {
     consumer.subscribe(List.of(src));int count=0;long end=System.nanoTime()+Duration.ofSeconds(35).toNanos();
     while(count==0&&System.nanoTime()<end)for(var r:consumer.poll(Duration.ofMillis(500))){require(Arrays.equals(r.value(),new byte[]{2,3}),"source payload");consumer.acknowledge(r,AcknowledgeType.REJECT);count++;}
     require(count==1,"missing/duplicate source");for(var result:consumer.commitSync().values())require(result.isEmpty(),result);
    }
    p=props(endpoint);p.put("key.deserializer",ByteArrayDeserializer.class.getName());p.put("value.deserializer",ByteArrayDeserializer.class.getName());p.put("enable.auto.commit","false");
    int count=0;try(KafkaConsumer<byte[],byte[]> consumer=new KafkaConsumer<>(p)) {
     TopicPartition tp=new TopicPartition(dst,0);consumer.assign(List.of(tp));consumer.seek(tp,0);long end=System.nanoTime()+Duration.ofSeconds(8).toNanos();
     while(System.nanoTime()<end)for(var r:consumer.poll(Duration.ofMillis(250))){require(r.offset()==0&&Arrays.equals(r.key(),new byte[]{1})&&Arrays.equals(r.value(),new byte[]{2,3}),"DLQ payload/offset");count++;}
    }
    require(count==level-1,"level"+level+" DLQ count "+count);
    TopicPartition tp=new TopicPartition(src,0);var info=admin.listShareGroupOffsets(Map.of(group,new ListShareGroupOffsetsSpec().topicPartitions(List.of(tp)))).partitionsToOffsetInfo(group).get().get(tp);
    require(info!=null&&info.lag().equals(Optional.of(0L)),info);System.out.println("STATE\tlevel"+level+"\t"+info);pass("share"+level+"-reject-dlq-count"+count+"-lag0");
   }
  }
 }
}
