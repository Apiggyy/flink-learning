package com.self.relearning.chapter09;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;


/**
 * 讲解MapState使用
 */
public class FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.print("input");
    
        dataStream.keyBy(Event::getUrl)
                .process(new FakeWindowResult(10 * 1000L))
                .print();
        env.execute();
    }
    
    public static class FakeWindowResult extends KeyedProcessFunction<String, Event, String> {
        
        private final long windowSize;
        
        private MapState<Long, Long> windowCountMapState;
    
        @Override
        public void open(Configuration parameters) throws Exception {
            windowCountMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("window-count-map", Long.class, Long.class)
            );
        }
    
        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }
    
        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            long windowStart = value.getTimestamp() / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;
    
            if (windowCountMapState.contains(windowStart)) {
                windowCountMapState.put(windowStart, windowCountMapState.get(windowStart) + 1);
            } else {
                windowCountMapState.put(windowStart, 1L);
                ctx.timerService().registerEventTimeTimer(windowEnd - 1);
            }
        }
    
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            long windowEnd = timestamp + 1L;
            long windowStart = windowEnd - windowSize;
            long count = windowCountMapState.get(windowStart);
            out.collect("窗口期：" + new Timestamp(windowStart) + " ~ " + new Timestamp(windowEnd) + " url {" +  ctx.getCurrentKey() + "} count => " + count);
            
            windowCountMapState.remove(windowStart);
        }
    }
}
