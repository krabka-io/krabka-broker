import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Isolated share2 fixture: deny inter-broker principal destination WRITE. */
public class DlqDeniedDestination {
 static Properties props(String endpoint) { Properties p=new Properties();p.put("bootstrap.servers",endpoint);p.put("security.protocol","SASL_PLAINTEXT");p.put("sasl.mechanism","PLAIN");p.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"fixture-password\";");p.put("default.api.timeout.ms","30000");p.put("request.timeout.ms","10000");return p; }
 static void require(boolean ok,Object detail) { if(!ok)throw new AssertionError(detail); }
 static void pass(String name) { System.out.println("SCENARIO\t"+name+"\tPASS"); }
 public static void main(String[] args)throws Exception {
  String endpoint=args[0];
  try(Admin admin=Admin.create(props(endpoint))) {
   var features=admin.describeFeatures().featureMetadata().get();System.out.println("FEATURES\t"+features);
   require(features.finalizedFeatures().get("share.version").maxVersionLevel()==2,"requires share2");
   {
    String suffix=UUID.randomUUID().toString(),src="src-"+suffix,dst="dlq."+suffix,group="grp-"+suffix;
    admin.createTopics(List.of(new NewTopic(src,1,(short)1),new NewTopic(dst,1,(short)1).configs(Map.of("errors.deadletterqueue.group.enable","true")))).all().get();
    List<AlterConfigOp> ops=new ArrayList<>();for(var e:Map.of("errors.deadletterqueue.topic.name",dst,"errors.deadletterqueue.copy.record.enable","true","share.auto.offset.reset","earliest").entrySet())ops.add(new AlterConfigOp(new ConfigEntry(e.getKey(),e.getValue()),AlterConfigOp.OpType.SET));
    admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP,group),ops)).all().get();

    var bindings=new ArrayList<org.apache.kafka.common.acl.AclBinding>();
    for(var type:List.of(org.apache.kafka.common.resource.ResourceType.CLUSTER,org.apache.kafka.common.resource.ResourceType.TOPIC,org.apache.kafka.common.resource.ResourceType.GROUP,org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID))
      bindings.add(new org.apache.kafka.common.acl.AclBinding(new org.apache.kafka.common.resource.ResourcePattern(type,type==org.apache.kafka.common.resource.ResourceType.CLUSTER?"kafka-cluster":"*",org.apache.kafka.common.resource.PatternType.LITERAL),new org.apache.kafka.common.acl.AccessControlEntry("User:ANONYMOUS","*",org.apache.kafka.common.acl.AclOperation.ALL,org.apache.kafka.common.acl.AclPermissionType.ALLOW)));
    bindings.add(new org.apache.kafka.common.acl.AclBinding(new org.apache.kafka.common.resource.ResourcePattern(org.apache.kafka.common.resource.ResourceType.TOPIC,dst,org.apache.kafka.common.resource.PatternType.LITERAL),new org.apache.kafka.common.acl.AccessControlEntry("User:ANONYMOUS","*",org.apache.kafka.common.acl.AclOperation.WRITE,org.apache.kafka.common.acl.AclPermissionType.DENY)));
    admin.createAcls(bindings).all().get();
    require(admin.describeAcls(org.apache.kafka.common.acl.AclBindingFilter.ANY).values().get().containsAll(bindings),"ACL visibility");
    System.out.println("ACL\t"+bindings);
    if(args.length>2 && args[2].equals("prepare"))return;
    Properties denied=new Properties();denied.put("bootstrap.servers",args[1]);denied.put("key.serializer",ByteArraySerializer.class.getName());denied.put("value.serializer",ByteArraySerializer.class.getName());denied.put("max.block.ms","10000");
    try(KafkaProducer<byte[],byte[]> probe=new KafkaProducer<>(denied)) {
      try {probe.send(new ProducerRecord<>(dst,new byte[]{1},new byte[]{9})).get();throw new AssertionError("denied principal wrote destination");}
      catch(java.util.concurrent.ExecutionException e){require(e.getCause() instanceof org.apache.kafka.common.errors.TopicAuthorizationException,e);System.out.println("DENIED\t"+e.getCause());}
    }
    pass("destination-write-denied-control");
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
    require(count==0,"denied DLQ count "+count);
    TopicPartition tp=new TopicPartition(src,0);var info=admin.listShareGroupOffsets(Map.of(group,new ListShareGroupOffsetsSpec().topicPartitions(List.of(tp)))).partitionsToOffsetInfo(group).get().get(tp);
    require(info!=null&&info.lag().equals(Optional.of(0L)),info);System.out.println("STATE\t"+info);pass("denied-destination-zero-records-lag0");
   }
  }
 }
}
