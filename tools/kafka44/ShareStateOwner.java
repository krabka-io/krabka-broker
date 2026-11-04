import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.*;
/** Caller-owned state coordinator fixture: explicit RF3 state placement and observed owner. */
public class ShareStateOwner {
 public static void main(String[] args) throws Exception {
  try(Admin a=Admin.create(ShareBoundary.props(args[0]))) {
   if(args[1].equals("setup")) {
    a.createTopics(List.of(new NewTopic("__share_group_state",Map.of(0,List.of(3,1,2))).configs(Map.of("min.insync.replicas","2","cleanup.policy","compact")))).all().get();
   }
   var t=a.describeTopics(List.of("__share_group_state")).allTopicNames().get().get("__share_group_state");
   if(t.partitions().size()!=1)throw new AssertionError(t);
   var p=t.partitions().get(0);
   if(p.leader()==null || p.replicas().size()!=3 || p.isr().size()<2)throw new AssertionError(t);
   System.out.println("STATE_OWNER\t"+p.leader().id()+"\t"+p);
  }
 }
}
