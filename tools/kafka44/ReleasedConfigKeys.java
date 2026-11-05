import java.util.TreeSet;
import org.apache.kafka.coordinator.group.GroupConfig;
import org.apache.kafka.storage.internals.log.LogConfig;
class ReleasedConfigKeys {
 public static void main(String[] args) {
  for (String name : new TreeSet<>(GroupConfig.configNames())) System.out.println("GROUP\t" + name);
  for (String name : new TreeSet<>(LogConfig.configNames())) System.out.println("TOPIC\t" + name);
 }
}
