import java.util.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.protocol.Errors;

/** Fresh caller-owned fixtures only; disable Streams before creating any group. */
public class StreamsMinimum extends StreamsPositive {
 public static void main(String[] args)throws Exception {
  ep=args[0];Properties p=new Properties();p.put("bootstrap.servers",ep);p.put("default.api.timeout.ms","30000");
  try(Admin admin=Admin.create(p)) {
   System.out.println("FEATURES\tbefore\t"+admin.describeFeatures().featureMetadata().get());
   admin.updateFeatures(Map.of("streams.version",new FeatureUpdate((short)0,FeatureUpdate.UpgradeType.SAFE_DOWNGRADE)),new UpdateFeaturesOptions()).all().get();
   var disabled=admin.describeFeatures().featureMetadata().get();System.out.println("FEATURES\tdisabled\t"+disabled);
   require(!disabled.finalizedFeatures().containsKey("streams.version")||disabled.finalizedFeatures().get("streams.version").maxVersionLevel()==0,disabled);
   var heartbeat=((StreamsGroupHeartbeatResponse)call(new StreamsGroupHeartbeatRequest(new StreamsGroupHeartbeatRequestData().setGroupId("minimum-disabled").setMemberId("member").setMemberEpoch(0),(short)1))).data();
   System.out.println("RESPONSE\tdisabled-heartbeat\t"+heartbeat);require(heartbeat.errorCode()==Errors.UNSUPPORTED_VERSION.code(),heartbeat);System.out.println("SCENARIO\tstreams0-heartbeat-unsupported\tPASS");
   var update=((StreamsGroupTopologyDescriptionUpdateResponse)call(new StreamsGroupTopologyDescriptionUpdateRequest(new StreamsGroupTopologyDescriptionUpdateRequestData().setGroupId("minimum-disabled").setMemberId("member").setTopologyEpoch(0),(short)0))).data();
   System.out.println("RESPONSE\tdisabled-topology-update\t"+update);require(update.errorCode()==Errors.UNSUPPORTED_VERSION.code(),update);System.out.println("SCENARIO\tstreams0-topology-update-unsupported\tPASS");
   admin.updateFeatures(Map.of("streams.version",new FeatureUpdate((short)1,FeatureUpdate.UpgradeType.UPGRADE)),new UpdateFeaturesOptions()).all().get();
   var enabled=admin.describeFeatures().featureMetadata().get();System.out.println("FEATURES\tenabled\t"+enabled);require(enabled.finalizedFeatures().get("streams.version").maxVersionLevel()==1,enabled);
  }
  StreamsPositive.main(new String[]{ep,"minimum"});
 }
}
