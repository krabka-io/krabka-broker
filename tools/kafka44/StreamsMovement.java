import java.util.*;
import java.nio.file.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;

/** Store on the owner, externally kill it, verify on a distinct elected owner. */
public class StreamsMovement extends StreamsFault {
 static FindCoordinatorResponseData.Coordinator owner()throws Exception {
  var data=((FindCoordinatorResponse)call(new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKeyType((byte)0).setCoordinatorKeys(List.of(group))).build((short)4))).data();
  require(data.coordinators().size()==1,data);var c=data.coordinators().get(0);require(c.errorCode()==0,c);System.out.println("OWNER\t"+c.nodeId()+"\t"+c.host()+":"+c.port());return c;
 }
 public static void main(String[] args)throws Exception {
  ep=args[0];Path identity=Path.of(args[1]);boolean prepare=args[2].equals("prepare");require(prepare||args[2].equals("recover"),"prepare/recover");
  if(prepare) {
   String suffix=UUID.randomUUID().toString();group="movement-"+suffix;topic="source-"+suffix;
   Properties p=new Properties();p.put("bootstrap.servers",ep);p.put("default.api.timeout.ms","30000");
   try(Admin admin=Admin.create(p)){admin.updateFeatures(Map.of("streams.version",new FeatureUpdate((short)1,FeatureUpdate.UpgradeType.UPGRADE)),new UpdateFeaturesOptions()).all().get();admin.createTopics(List.of(new NewTopic(topic,1,(short)3).configs(Map.of("min.insync.replicas","2")))).all().get();}
   var coordinator=owner();ep=coordinator.host()+":"+coordinator.port();var m=join("movement-member");require(m.topologyDescriptionRequired(),m);require(update(m.memberId()).errorCode()==0,"store");
   var d=describe();require(d.topologyDescriptionStatus()==3&&d.topologyDescription()!=null,d);require(d.topologyDescription().subtopologies().get(0).nodes().get(0).sourceTopics().equals(List.of(topic)),d);
   var leave=((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId(m.memberId()).setMemberEpoch(-1),(short)1))).data();require(leave.errorCode()==0,leave);
   Files.writeString(identity,group+"\n"+topic+"\n"+coordinator.nodeId()+"\n");pass("stored-exact-source-before-owner-loss");
  } else {
   var lines=Files.readAllLines(identity);require(lines.size()==3,lines);group=lines.get(0);topic=lines.get(1);var coordinator=owner();require(coordinator.nodeId()!=Integer.parseInt(lines.get(2)),"owner did not move");ep=coordinator.host()+":"+coordinator.port();pass("coordinator-owner-changed");
   var d=describe();require(d.topologyDescriptionStatus()==3&&d.topologyDescription()!=null,d);require(d.topologyDescription().subtopologies().get(0).nodes().get(0).sourceTopics().equals(List.of(topic)),d);pass("stored-exact-source-after-owner-loss");
   require(delete((short)3)==0,"delete on new owner");pass("delete-on-new-owner");var recreated=join("recreated-movement-member");require(recreated.topologyDescriptionRequired(),recreated);var clean=describe();require(clean.topologyDescriptionStatus()==1&&clean.topologyDescription()==null,clean);pass("recreated-description-absent");
  }
 }
}
