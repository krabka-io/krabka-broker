import java.util.*;
import java.nio.file.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.protocol.Errors;
public class StreamsFault extends StreamsPositive {
 static Path root;
 static void mode(String op,String value)throws Exception{Files.writeString(root.resolve(op),value);}
 static void pass(String name){System.out.println("SCENARIO\t"+name+"\tPASS");}
 static StreamsGroupTopologyDescriptionUpdateResponseData update(String member)throws Exception {
  var desc=new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription().setSubtopologies(List.of(new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology().setSubtopologyId("0").setNodes(List.of(new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode().setName("source-node").setNodeType((byte)1).setSourceTopics(List.of(topic))))));
  var d=((StreamsGroupTopologyDescriptionUpdateResponse)call(new StreamsGroupTopologyDescriptionUpdateRequest(new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId(group).setMemberId(member).setTopologyEpoch(0).setTopologyDescription(desc),(short)0))).data();System.out.println("RESPONSE\tupdate\t"+d);return d;
 }
 static short delete(short version)throws Exception{var d=((DeleteGroupsResponse)call(new DeleteGroupsRequest(new DeleteGroupsRequestData().setGroupsNames(List.of(group)),version))).data();System.out.println("RESPONSE\tdelete\t"+d);return d.results().iterator().next().errorCode();}
 public static void main(String[]a)throws Exception{
  ep=a[0];root=Path.of(a[1]);String suffix=UUID.randomUUID().toString();group="fault-"+suffix;topic="source-"+suffix;
  mode("get","ok");mode("set","ok");mode("delete","ok");
  Properties p=new Properties();p.put("bootstrap.servers",ep);p.put("default.api.timeout.ms","20000");p.put("request.timeout.ms","10000");try(Admin admin=Admin.create(p)){admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get();}
  call(new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKeyType((byte)0).setCoordinatorKeys(List.of(group))).build((short)4));
  var member=join("fault-member");require(member.topologyDescriptionRequired(),member);pass("initial-solicitation");
  mode("set","transient");require(update(member.memberId()).errorCode()==Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(),"transient set");pass("transient-set-failed");
  var missing=describe();require(missing.topologyDescription()==null,missing);pass("transient-set-no-description");
  mode("set","ok");require(update(member.memberId()).errorCode()==0,"repair set");var stored=describe();require(stored.topologyDescriptionStatus()==3 && stored.topologyDescription()!=null,stored);pass("transient-set-repair-stored");
  mode("get","transient");var failed=describe();require(failed.topologyDescriptionStatus()==2 && failed.topologyDescription()==null,failed);pass("get-error-not-missing");
  mode("get","ok");require(describe().topologyDescriptionStatus()==3,"get repaired");pass("get-repair-stored");
  var leave=((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId(member.memberId()).setMemberEpoch(-1),(short)1))).data();require(leave.errorCode()==0,leave);
  mode("delete","transient");require(delete((short)2)==Errors.UNKNOWN_SERVER_ERROR.code(),"legacy delete failure");pass("legacy-delete-failure-unknown-server");require(delete((short)3)==Errors.GROUP_DELETION_FAILED.code(),"delete failure");pass("delete-failure-reported");require(describe().errorCode()==0,"group retained");pass("delete-failure-group-retained");
  mode("delete","ok");require(delete((short)3)==0,"delete repair");pass("delete-repair-success");var recreated=join("recreated");require(recreated.topologyDescriptionRequired(),recreated);require(describe().topologyDescriptionStatus()==1,"recreated clean");pass("delete-recreate-clean");
  mode("set","permanent");require(update(recreated.memberId()).errorCode()==Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code(),"permanent failure");pass("permanent-set-failed");var permanent=describe();require(permanent.topologyDescriptionStatus()==1 && permanent.topologyDescription()==null,permanent);pass("permanent-set-status");
 }
}
