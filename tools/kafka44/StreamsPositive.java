import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;
public class StreamsPositive {
 static String ep,group,topic;
 static void require(boolean b,Object msg){if(!b)throw new IllegalStateException("ASSERT: "+msg);}
 static AbstractResponse call(AbstractRequest r)throws Exception{for(int i=0;i<40;i++){var x=ProtocolScenarios.exchange(ep,r);if(!x.errorCounts().keySet().stream().anyMatch(e->e.code()==14||e.code()==15||e.code()==16))return x;System.out.println("RETRY\t"+x.data());Thread.sleep(250);}throw new IllegalStateException("coordinator unavailable");}
 static StreamsGroupHeartbeatResponseData join(String member)throws Exception{
  var top=new StreamsGroupHeartbeatRequestData.Topology().setEpoch(0).setSubtopologies(List.of(new StreamsGroupHeartbeatRequestData.Subtopology().setSubtopologyId("0").setSourceTopics(List.of(topic))));
  var r=new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId(member).setMemberEpoch(0).setProcessId(member).setRebalanceTimeoutMs(30000).setActiveTasks(List.of()).setStandbyTasks(List.of()).setWarmupTasks(List.of()).setTopology(top);
  var d=((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(r,(short)1))).data();System.out.println("RESPONSE\tjoin\t"+d);require(d.errorCode()==0,d);return d;
 }
 static StreamsGroupDescribeResponseData.DescribedGroup describe()throws Exception{
  var d=((StreamsGroupDescribeResponse)call(new StreamsGroupDescribeRequest(new StreamsGroupDescribeRequestData().setGroupIds(List.of(group)).setIncludeTopologyDescription(true),(short)1))).data();System.out.println("RESPONSE\tdescribe\t"+d);require(d.groups().size()==1,d);var g=d.groups().get(0);require(g.errorCode()==0,g);return g;
 }
 public static void main(String[]a)throws Exception{
 ep=a[0];String suffix=a[1]+"-"+UUID.randomUUID().toString().substring(0,8);group="positive-"+suffix;topic="source-"+suffix;Properties p=new Properties();p.put("bootstrap.servers",ep);p.put("default.api.timeout.ms","20000");p.put("request.timeout.ms","10000");
 try(Admin admin=Admin.create(p)){admin.createTopics(List.of(new NewTopic(topic,1,(short)1))).all().get();}
 var fc=(FindCoordinatorResponse)call(new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData().setKeyType((byte)0).setCoordinatorKeys(List.of(group))).build((short)4));System.out.println("RESPONSE\tfind\t"+fc.data());
 var first=join("first-"+suffix);require(first.topologyDescriptionRequired(),first);System.out.println("SCENARIO\tfirst-solicited\tPASS");
 var second=join("second-"+suffix);require(!second.topologyDescriptionRequired(),second);System.out.println("SCENARIO\tsecond-not-solicited\tPASS");
 var before=describe();require(before.topologyDescriptionStatus()==1 && before.topologyDescription()==null,before);System.out.println("SCENARIO\tinitial-not-stored\tPASS");
 var desc=new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription().setSubtopologies(List.of(new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionSubtopology().setSubtopologyId("0").setNodes(List.of(new StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescriptionNode().setName("source-node").setNodeType((byte)1).setSourceTopics(List.of(topic))))));
 var update=((StreamsGroupTopologyDescriptionUpdateResponse)call(new StreamsGroupTopologyDescriptionUpdateRequest(new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId(group).setMemberId(first.memberId()).setTopologyEpoch(0).setTopologyDescription(desc),(short)0))).data();System.out.println("RESPONSE\tupdate\t"+update);require(update.errorCode()==0,update);System.out.println("SCENARIO\tpush-success\tPASS");
 var after=describe();require(after.topologyDescriptionStatus()==3 && after.topologyDescription()!=null,after);require(after.topologyDescription().subtopologies().get(0).nodes().get(0).sourceTopics().equals(List.of(topic)),after);System.out.println("SCENARIO\tstored-exact-source\tPASS");
 for(var member:List.of(first,second)) {var leave=((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(new StreamsGroupHeartbeatRequestData().setGroupId(group).setMemberId(member.memberId()).setMemberEpoch(-1),(short)1))).data();System.out.println("RESPONSE\tleave\t"+leave);require(leave.errorCode()==0,leave);}System.out.println("SCENARIO\tleave-members\tPASS");
 var deletion=((DeleteGroupsResponse)call(new DeleteGroupsRequest(new DeleteGroupsRequestData().setGroupsNames(List.of(group)),(short)2))).data();System.out.println("RESPONSE\tdelete\t"+deletion);require(deletion.results().size()==1 && deletion.results().iterator().next().errorCode()==0,deletion);System.out.println("SCENARIO\tdelete-group\tPASS");
 var recreated=join("recreated-"+suffix);require(recreated.topologyDescriptionRequired(),recreated);System.out.println("SCENARIO\trecreated-solicited\tPASS");var empty=describe();require(empty.topologyDescriptionStatus()==1 && empty.topologyDescription()==null,empty);System.out.println("SCENARIO\trecreated-not-stored\tPASS");

 }
}
