package com.self.relearning.chapter09;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 讲解值状态使用
 */
public class PeriodPvExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.print("input");
        dataStream
                .keyBy(Event::getUser)
                .process(new PeriodicPvResult())
                .print();
        env.execute();
    }
    
    public static class PeriodicPvResult extends KeyedProcessFunction<String, Event, String> {
    
        ValueState<Long> countState;
        ValueState<Long> timeTsState;
        
        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            long oldValue = countState.value() == null ? 0 : countState.value();
            countState.update(oldValue + 1);
            
            if(timeTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getTimestamp() + 10 * 1000L);
                timeTsState.update(value.getTimestamp() + 10 * 1000L);
            }
        }
    
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "'s pv: " + countState.value());
            timeTsState.clear();
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timeTsState.update(timestamp + 10 * 1000L);
        }
    
        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("count", Long.class)
            );
    
            timeTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timeTs", Long.class)
            );
        }
    }
}
