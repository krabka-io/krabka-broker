import org.apache.kafka.server.share.dlq.*;
import java.util.concurrent.*;
import java.nio.file.*;
/** A real custom manager loaded only from the fixture JAR. */
public final class ExtensionFixture implements ShareGroupDLQManager {
    int calls;
    public CompletableFuture<Void> enqueue(ShareGroupDLQRecordParameter r) {
        if (r.groupId().equals("fail")) return CompletableFuture.failedFuture(new IllegalStateException("fixture failure"));
        if (r.groupId().equals("hang")) return new CompletableFuture<>();
        if (r.firstOffset() != 9007199254740993L || r.lastOffset() != Long.MAX_VALUE || r.deliveryCount().orElseThrow() != 7 || r.topicIdPartition().partition() != 3 || !r.topicIdPartition().topicId().toString().equals("ABEiM0RVZneImaq7zN3u_w")) throw new AssertionError("conversion");
        if (r.cause().orElseThrow() != (calls++ == 0 ? CLIENT_REJECT : DELIVERY_COUNT_EXCEEDED)) throw new AssertionError("cause or state lifetime");
        return CompletableFuture.completedFuture(null);
    }
    public void stop() { if (calls != 2) throw new IllegalStateException("two callbacks required before stop"); }

    public static class PermanentStorageFixture extends org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin {
    public PermanentStorageFixture() {}
    @Override public CompletableFuture<Void> setTopology(String group, int epoch, org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription topology) {
        return CompletableFuture.failedFuture(new org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException("permanent fixture rejection"));
    }
}

}
