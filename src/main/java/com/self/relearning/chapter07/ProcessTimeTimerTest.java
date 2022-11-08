package com.self.relearning.chapter07;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource());
        dataStream.keyBy(Event::getUser)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        long currentProcessingTime = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() + " 数据到达，达到时间: " + new Timestamp(currentProcessingTime));
                    
                        ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 10 * 1000L);
                    }
                
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 定时器触发，触发时间: " + new Timestamp(timestamp));
                    }
                }).print();
        env.execute();
    }
}
