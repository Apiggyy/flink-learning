package com.self.relearning.chapter07;

import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.keyBy(Event::getUser)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long ts = value.getTimestamp();
                        out.collect(ctx.getCurrentKey() + " 数据到达，时间戳: " + new Timestamp(ts) + ", watermark: " + ctx.timerService().currentWatermark());
                        ctx.timerService().registerEventTimeTimer(ts + 10 * 1000L);
                    }
                
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间: " + new Timestamp(timestamp) + ", watermark: " + ctx.timerService().currentWatermark());
                    }
                }).print();
        env.execute();
    }
    
    private static class CustomSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            ctx.collect(new Event("Mary", "./home", 1000L));
            Thread.sleep(5000L);
            ctx.collect(new Event("Bob", "./home", 11000L));
            Thread.sleep(5000L);
            ctx.collect(new Event("Alice", "./home", 11001L));
            Thread.sleep(5000L);
            ctx.collect(new Event("Lucy", "./home", 21000L));
            Thread.sleep(5000L);
            ctx.collect(new Event("Jack", "./home", 21001L));
            Thread.sleep(5000L);
        }
    
        @Override
        public void cancel() {
        
        }
    }
}
