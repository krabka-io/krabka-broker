import java.io.*;
import java.nio.file.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.kafka.server.log.remote.storage.*;
/** Copy-only qualification backend; never use for production storage. */
public class RecordingStorage implements RemoteStorageManager {
    private Path root;
    public void configure(Map<String, ?> config) { root = Path.of(config.get("dir").toString()); }
    private Path dir(RemoteLogSegmentMetadata m) { return root.resolve(m.remoteLogSegmentId().id().toString()); }
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(RemoteLogSegmentMetadata m, LogSegmentData d) throws RemoteStorageException {
        try {
            Path target=dir(m); Files.createDirectories(target);
            Files.copy(d.logSegment(),target.resolve("log"),StandardCopyOption.REPLACE_EXISTING);
            Files.copy(d.offsetIndex(),target.resolve("OFFSET"),StandardCopyOption.REPLACE_EXISTING);
            Files.copy(d.timeIndex(),target.resolve("TIMESTAMP"),StandardCopyOption.REPLACE_EXISTING);
            Files.copy(d.producerSnapshotIndex(),target.resolve("PRODUCER_SNAPSHOT"),StandardCopyOption.REPLACE_EXISTING);
            if(d.transactionIndex().isPresent()) Files.copy(d.transactionIndex().get(),target.resolve("TRANSACTION"),StandardCopyOption.REPLACE_EXISTING);
            ByteBuffer epoch=d.leaderEpochIndex().duplicate(); byte[] bytes=new byte[epoch.remaining()];epoch.get(bytes);Files.write(target.resolve("LEADER_EPOCH"),bytes);
            Files.writeString(root.resolve("copies.tsv"),System.currentTimeMillis()+"\t"+m.maxTimestampMs()+"\t"+m.startOffset()+"\t"+m.endOffset()+"\t"+m.topicIdPartition()+"\n",StandardOpenOption.CREATE,StandardOpenOption.APPEND);
            return Optional.empty();
        } catch(Exception e) {throw new RemoteStorageException(e);}
    }
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata m,int start) throws RemoteStorageException {return fetchLogSegment(m,start,Integer.MAX_VALUE);}
    public InputStream fetchLogSegment(RemoteLogSegmentMetadata m,int start,int end) throws RemoteStorageException {
        try {InputStream in=Files.newInputStream(dir(m).resolve("log"));in.skipNBytes(start);return in;} catch(Exception e){throw new RemoteStorageException(e);}
    }
    public InputStream fetchIndex(RemoteLogSegmentMetadata m,IndexType t) throws RemoteStorageException {
        try{return Files.newInputStream(dir(m).resolve(t.name()));}catch(Exception e){throw new RemoteStorageException(e);}
    }
    public void deleteLogSegmentData(RemoteLogSegmentMetadata m) throws RemoteStorageException {throw new RemoteStorageException("fixture never deletes archives");}
    public void close() {}
}
