import java.util.*;
import java.nio.file.*;
import org.apache.kafka.clients.admin.*;
/** Clean-restart storage identity for an isolated native filesystem-backed broker. */
public class StreamsStorageRestart extends StreamsFault {
 public static void main(String[]a)throws Exception{
  ep=a[0];root=Path.of(a[1]);Path identity=root.resolve("restart-identity.properties");
  if(a[2].equals("prepare")) {
   mode("set","ok");mode("get","ok");mode("delete","ok");
   String suffix=UUID.randomUUID().toString();group="restart-"+suffix;topic="source-"+suffix;
   Properties p=new Properties();p.put("bootstrap.servers",ep);p.put("default.api.timeout.ms","20000");p.put("request.timeout.ms","10000");
   try(Admin admin=Admin.create(p)){admin.updateFeatures(Map.of("streams.version",new FeatureUpdate((short)1,FeatureUpdate.UpgradeType.UPGRADE)),new UpdateFeaturesOptions()).all().get();admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get();}
   call(new org.apache.kafka.common.requests.FindCoordinatorRequest.Builder(new org.apache.kafka.common.message.FindCoordinatorRequestData().setKeyType((byte)0).setCoordinatorKeys(List.of(group))).build((short)4));
   var m=join("persistent-member");require(update(m.memberId()).errorCode()==0,"store before restart");
   Files.writeString(identity,group+"\n"+topic+"\n");
  } else {
   require(a[2].equals("recover"),"mode prepare/recover");var lines=Files.readAllLines(identity);require(lines.size()==2,"restart identity");group=lines.get(0);topic=lines.get(1);
  }
  var d=describe();require(d.topologyDescriptionStatus()==3 && d.topologyDescription()!=null,d);
  require(d.topologyDescription().subtopologies().get(0).nodes().get(0).sourceTopics().equals(List.of(topic)),d);
  pass(a[2].equals("prepare")?"stored-before-restart":"exact-description-after-restart");
 }
}
