import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

/** Physical archive regression: another topic's files cannot satisfy this probe. */
public class CopyArchiveScopeTest {
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]);
        String topic = "scope-topic";
        Set<Path> expected = Set.of(root.resolve(topic + "/segment/log"), root.resolve(topic + "-0-id/segment.log"));
        for (String relative : new String[]{"copies.tsv", "unrelated/log", "scope-topic-other/log", "scope-topic-1-id/log", "scope-topic/segment/log", "scope-topic-0-id/segment.log"}) {
            Path path = root.resolve(relative);
            Files.createDirectories(path.getParent());
            Files.writeString(path, "fixture");
        }
        if (!CopyProbe.files(root, topic).equals(expected)) throw new IllegalStateException("archive scoping included another topic");
        System.out.println("archive-scope regression PASS");
    }
}
