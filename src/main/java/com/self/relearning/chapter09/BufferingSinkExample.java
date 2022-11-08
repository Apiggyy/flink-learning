package com.self.relearning.chapter09;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000L);
//        env.setStateBackend(new HashMapStateBackend());
        // 配置存储检查点到JobManager堆内存
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        checkpointConfig.setCheckpointTimeout(60000L);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(500L); // 这个参数设置了MaxConcurrentCheckpoints的参数只能是1，优先级高
        checkpointConfig.setMaxConcurrentCheckpoints(1); // 优先级低
        checkpointConfig.enableUnalignedCheckpoints();  // 开启不对齐的检查点保存方式,要求检查点方式是EXACTLY_ONCE, 并发数量是1
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // 默认取消任务会删除检查点文件，可以设置保留
        checkpointConfig.setTolerableCheckpointFailureNumber(0);  // 默认是0，不允许检查点失败
        
        // 保存到文件系统
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://"));
        
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.print("input");
        dataStream.addSink(new BufferingSink(10));
        
        env.execute();
    }
    
    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        
        private final int threshold;
        
        private final List<Event> bufferedElements ;
        
        private ListState<Event> checkpointedState;
    
        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }
    
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            for (Event element : bufferedElements) {
                checkpointedState.add(element);
            }
        }
    
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<Event>("buffered-elements", Event.class);
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);
            
            //如果从故障恢复，需要将ListState中的元素复制到列表中
            if (context.isRestored()) {
                for (Event event : checkpointedState.get()) {
                    bufferedElements.add(event);
                }
            }
            
        }
    
        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferedElements.add(value);
            if(bufferedElements.size() == threshold) {
                for (Event element : bufferedElements) {
                    System.out.println(element);
                }
                System.out.println("===============输出完毕===============");
                bufferedElements.clear();
            }
        }
    }
}
