import java.util.*;
import javax.management.*;
import javax.management.remote.*;

/** Read actual released broker meters after ShareBoundary, not injected counters. */
public class JmxCounts {
  public static void main(String[] args) throws Exception {
    try(JMXConnector connection=JMXConnectorFactory.connect(new JMXServiceURL(args[0]))) {
      var server=connection.getMBeanServerConnection();
      var beans=new TreeSet<ObjectName>(Comparator.comparing(ObjectName::getCanonicalName));
      for(var bean:server.queryNames(null,null))if(bean.getCanonicalName().contains("DeadLetterQueue"))beans.add(bean);
      if(beans.isEmpty())throw new AssertionError("No DLQ meters found");
      for(var bean:beans)System.out.println(bean.getCanonicalName()+" Count="+server.getAttribute(bean,"Count"));
      for(boolean copy:new boolean[]{true,false})for(int step=0;step<4;step++) {
        String group="grp"+args[1]+(copy?"t":"f")+step;
        long produce=0,failed=0,written=0;
        for(var bean:beans)if(bean.getCanonicalName().contains(group)) {
          long count=((Number)server.getAttribute(bean,"Count")).longValue();
          String name=bean.getKeyProperty("name");
          if(name.equals("DeadLetterQueueTotalProduceRequestsPerSec"))produce=count;
          if(name.equals("DeadLetterQueueFailedProduceRequestsPerSec"))failed=count;
          if(name.equals("DeadLetterQueueRecordCount"))written=count;
        }
        if(produce!=1 || failed!=(step==1?1:0) || written!=(step==1?0:1))throw new AssertionError(group+" produce="+produce+" failed="+failed+" written="+written);
        System.out.printf("ASSERT group=%s produce=%d failed=%d written=%d%n",group,produce,failed,written);
      }
    }
  }
}
