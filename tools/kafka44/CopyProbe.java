import java.util.*;
import java.nio.file.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.ConfigResource;
/** Isolated released-client copy-delay/dynamic-update smoke; uses a fresh topic and counts new archive files. */
public class CopyProbe {
 static Set<Path> files(Path root, String topic) throws Exception {
  try(var walk=Files.walk(root)) { return walk.filter(Files::isRegularFile).filter(p -> {String first=root.relativize(p).getName(0).toString();return first.equals(topic)||first.startsWith(topic+"-0-");}).collect(java.util.stream.Collectors.toSet()); }
 }
 public static void main(String[] args)throws Exception {
  String mode=args.length==3?args[2]:"dynamic-zero";
  if(!List.of("dynamic-zero","age","bytes","derived-age").contains(mode))throw new IllegalArgumentException("unknown copy scenario");
  String endpoint=args[0], topic="copy-probe-"+UUID.randomUUID(); Path archive=Path.of(args[1]);
  Properties cfg=new Properties();cfg.put("bootstrap.servers",endpoint);cfg.put("default.api.timeout.ms","30000");
  Set<Path> baseline=files(archive,topic);
  try(Admin admin=Admin.create(cfg)) {
   Map<String,String> configs=Map.of("remote.storage.enable","true","segment.bytes","1048576","max.message.bytes","131072","remote.copy.lag.ms",mode.equals("age")?"6000":mode.equals("derived-age")?"-1":"60000","remote.copy.lag.bytes",mode.equals("bytes")?"1048576":"100000000","local.retention.ms",mode.equals("derived-age")?"6000":"120000","local.retention.bytes","200000000");
   admin.createTopics(List.of(new NewTopic(topic,1,(short)1).configs(configs))).all().get();
   cfg.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");cfg.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");cfg.put("acks","all");cfg.put("linger.ms","0");
   try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(cfg)) {for(int n=0;n<24;n++) producer.send(new ProducerRecord<>(topic,new byte[]{(byte)n},new byte[65536])).get();}
   Thread.sleep(3000);
   Set<Path> beforeFiles=files(archive,topic);beforeFiles.removeAll(baseline);long before=beforeFiles.size();
   System.out.println("BEFORE\t"+before+"\t"+topic);
   if(before!=0)throw new IllegalStateException("copy occurred before age/byte thresholds");
   if(mode.equals("dynamic-zero")) admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC,topic),List.of(new AlterConfigOp(new ConfigEntry("remote.copy.lag.ms","0"),AlterConfigOp.OpType.SET)))).all().get();
   if(mode.equals("bytes")) {
    try(KafkaProducer<byte[],byte[]> producer=new KafkaProducer<>(cfg)) {for(int n=24;n<48;n++) producer.send(new ProducerRecord<>(topic,new byte[]{(byte)n},new byte[65536])).get();}
   }
   for(int attempt=0;attempt<120;attempt++) {
    Set<Path> afterFiles=files(archive,topic);afterFiles.removeAll(baseline);long after=afterFiles.size();
    if(after>0){System.out.println("DETAIL\t"+topic+"\tfiles="+after);System.out.println("SCENARIO\tdelay-then-"+mode+"-copies\tPASS");return;}
    Thread.sleep(500);
   }
   throw new IllegalStateException("dynamic zero lag did not produce an archive");
  }
 }
}
