import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
/** Observe actual assigned replicas, ISR membership and effective min ISR. */
public class Rf3Inspect {
 public static void main(String[] args) throws Exception {
  try(Admin admin=Admin.create(ShareBoundary.props(args[0]))) {
   var topics=List.of("source-rf3crash08","dlq.rf3crash08","__share_group_state");
   var metadata=admin.describeTopics(topics).allTopicNames().get();
   var resources=topics.stream().map(t->new ConfigResource(ConfigResource.Type.TOPIC,t)).toList();
   var configs=admin.describeConfigs(resources).all().get();
   for(var t:topics) {
    var p=metadata.get(t).partitions().get(0);var min=configs.get(new ConfigResource(ConfigResource.Type.TOPIC,t)).get("min.insync.replicas").value();
    if(p.replicas().size()!=3||p.isr().size()!=3||!min.equals("2"))throw new AssertionError(t+" "+p+" minISR="+min);
    System.out.println("RF3_ISR3_MINISR2 topic="+t+" leader="+p.leader().id()+" replicas="+p.replicas()+" isr="+p.isr());
   }
  }
 }
}
