package org.apache.kafka.server.share.dlq;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.*;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.record.internal.*;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.util.timer.Timer;

/** Released-binary component probe, NOT broker/network/recovery qualification. */
public class Boundary {
    static int limit;
    static int retries;
    static class Metrics extends ShareGroupMetrics {
        int failed, records;
        Metrics() { super(Time.SYSTEM); }
        public void recordDLQProduceFailed(String group) { failed++; }
        public void recordDLQRecordWrite(String group, int count) { records+=count; }
    }
    static Object proxy(Class<?> c) {
        return Proxy.newProxyInstance(c.getClassLoader(), new Class<?>[]{c}, (p,m,a) -> {
            if (m.getName().equals("add")) { retries++; return null; }
            if (m.getName().equals("dlqTopicMaxMessageBytes")) return limit;
            if (m.getReturnType() == boolean.class) return false;
            if (m.getReturnType() == int.class) return 0;
            return null;
        });
    }
    static void set(Object o, String name, Object value) throws Exception {
        Field f = o.getClass().getDeclaredField(name); f.setAccessible(true); f.set(o,value);
    }
    public static void main(String[] args) throws Exception {
        try (Metrics metrics = new Metrics()) {
            var manager = new ShareGroupDLQStateManager((KafkaClient)proxy(KafkaClient.class),
                (ShareGroupDLQMetadataCacheHelper)proxy(ShareGroupDLQMetadataCacheHelper.class),
                Time.SYSTEM, (Timer)proxy(Timer.class), metrics, (LogReader)proxy(LogReader.class));
            var param = new ShareGroupDLQRecordParameter("g1", new TopicIdPartition(Uuid.ZERO_UUID,3,"orders"),4,4,
                Optional.of((short)5),Optional.of(ShareGroupDLQManager.CLIENT_REJECT));
            var handler = manager.new ProduceRequestHandler(param,new CompletableFuture<>(),1000,30000,5);
            set(handler,"dlqTopicPartitionData", new ShareGroupDLQMetadataCacheHelper.TopicPartitionData("dlq.orders",Optional.of(1),Optional.of(Uuid.ZERO_UUID),List.of()));
            var source = MemoryRecords.withRecords(org.apache.kafka.common.compress.Compression.NONE,
                new SimpleRecord(0,"k".getBytes(),new byte[100]));
            var record = source.records().iterator().next();
            for (boolean copy : new boolean[]{true,false}) {
                set(handler,"resolvedRecordData",copy ? Map.of(4L,record) : Map.of());
                limit = 1048588;
                int size = handler.topicProduceData().partitionData().get(0).records().sizeInBytes();
                for (int boundary : new int[]{size-1,size,size+1}) {
                    limit=boundary;
                    var buf=handler.topicProduceData().partitionData().get(0).records();
                    var batch=(MemoryRecords)buf;
                    var output=batch.records().iterator().next();
                    if (output.hasValue()!=copy || batch.sizeInBytes()!=size) throw new AssertionError("payload changed");
                    System.out.printf("copy=%s limit=%d batch_bytes=%d value_bytes=%d oversize=%s%n",copy,boundary,batch.sizeInBytes(),output.valueSize(),size>boundary);
                }
            }
            for (Errors error : new Errors[]{Errors.NONE,Errors.MESSAGE_TOO_LARGE,Errors.NOT_LEADER_OR_FOLLOWER}) {
                var future = new CompletableFuture<Void>();
                var h = manager.new ProduceRequestHandler(param,future,1000,30000,5);
                set(h,"dlqTopicPartitionData",new ShareGroupDLQMetadataCacheHelper.TopicPartitionData("dlq.orders",Optional.of(1),Optional.of(Uuid.ZERO_UUID),List.of()));
                limit=1048588; h.topicProduceData();
                var data=new ProduceResponseData();
                data.responses().add(new ProduceResponseData.TopicProduceResponse().setTopicId(Uuid.ZERO_UUID)
                    .setPartitionResponses(List.of(new ProduceResponseData.PartitionProduceResponse().setIndex(0).setErrorCode(error.code()))));
                var response=new ClientResponse(new RequestHeader(ApiKeys.PRODUCE,(short)13,"probe",1),null,"1",0,0,false,null,null,new ProduceResponse(data));
                var method=h.getClass().getDeclaredMethod("handleProduceResponse",ClientResponse.class); method.setAccessible(true);
                metrics.failed=0; metrics.records=0; retries=0;
                method.invoke(h,response);
                if (error==Errors.MESSAGE_TOO_LARGE && (!future.isCompletedExceptionally() || metrics.failed!=1 || retries!=0 || metrics.records!=0)) throw new AssertionError("terminal oversize");
                if (error==Errors.NONE && (!future.isDone() || metrics.records!=1)) throw new AssertionError("success");
                if (error==Errors.NOT_LEADER_OR_FOLLOWER && (future.isDone() || retries!=1)) throw new AssertionError("retry");
                System.out.printf("injected_error=%s complete=%s exceptional=%s failed_meter=%d written_meter=%d scheduled_retries=%d%n",error,future.isDone(),future.isCompletedExceptionally(),metrics.failed,metrics.records,retries);
            }

        }
    }
}
