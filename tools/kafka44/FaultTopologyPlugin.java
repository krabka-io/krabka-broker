import java.util.concurrent.*;
import java.nio.file.*;
import java.util.Base64;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.*;
public class FaultTopologyPlugin extends InMemoryTopologyDescriptionPlugin {
 static final ObjectMapper JSON = new ObjectMapper().activateDefaultTyping(com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator.builder().allowIfSubType("org.apache.kafka.coordinator.group.api.streams.").allowIfSubType("java.util.").build(), ObjectMapper.DefaultTyping.NON_FINAL).setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.FIELD, com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY);
 static Path file(String group,int epoch) { return Path.of("/evidence/store",Base64.getUrlEncoder().withoutPadding().encodeToString(group.getBytes(java.nio.charset.StandardCharsets.UTF_8))+"-"+epoch+".json"); }
 static String mode(String op){try {return Files.readString(Path.of("/evidence/"+op)).trim();}catch(Exception e){return "ok";}}
 static <T> CompletableFuture<T> fail(String m){return CompletableFuture.failedFuture(m.equals("permanent") ? new StreamsTopologyDescriptionPermanentFailureException("injected-permanent") : new StreamsTopologyDescriptionTransientFailureException("injected-transient"));}
 public CompletableFuture<Void> setTopology(String g,int e,StreamsGroupTopologyDescription d){String m=mode("set");System.out.println("FAULT_SPI set "+g+" "+m);if(!m.equals("ok"))return fail(m);
 try { Files.createDirectories(file(g,e).getParent());Path tmp=Files.createTempFile(file(g,e).getParent(),"topology-",".tmp");JSON.writeValue(tmp.toFile(),d);Files.move(tmp,file(g,e),StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);return CompletableFuture.completedFuture(null); }catch(Exception ex){return CompletableFuture.failedFuture(ex);}}
 public CompletableFuture<StreamsGroupTopologyDescription> getTopology(String g,int e){String m=mode("get");System.out.println("FAULT_SPI get "+g+" "+m);if(!m.equals("ok"))return fail(m);
 try { return CompletableFuture.completedFuture(Files.exists(file(g,e))?JSON.readValue(file(g,e).toFile(),StreamsGroupTopologyDescription.class):null); }catch(Exception ex){return CompletableFuture.failedFuture(ex);}}
 public CompletableFuture<Void> deleteTopology(String g){String m=mode("delete");System.out.println("FAULT_SPI delete "+g+" "+m);if(!m.equals("ok"))return fail(m);
 try { Path dir=file(g,0).getParent();if(Files.isDirectory(dir)){String prefix=file(g,0).getFileName().toString().replace("-0.json", "-");try(var paths=Files.list(dir)){for(Path p:paths.filter(p->p.getFileName().toString().startsWith(prefix)).toList())Files.delete(p);}}return CompletableFuture.completedFuture(null); }catch(Exception ex){return CompletableFuture.failedFuture(ex);}}
}
