package com.self.relearning.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order-1", "app", 1000L),
                Tuple3.of("order-2", "app", 3000L),
                Tuple3.of("order-3", "app", 2000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.f2)
        );
        
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                Tuple4.of("order-1", "third-party", "success", 1500L),
                Tuple4.of("order-2", "third-party", "success", 3500L),
                Tuple4.of("order-4", "third-party", "success", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.f3)
        );
//        appStream
//                .keyBy(data -> data.f0)
//                .connect(thirdPartyStream.keyBy(data -> data.f0))
//                .process(new OrderMatchResult());
        
        appStream
                .connect(thirdPartyStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();
    
        env.execute();
    }
    
    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {
        // 定义状态变量，保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;
    
        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("third-party-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }
    
        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if(thirdPartyEventState.value() != null) {
                out.collect("对账成功：" + value + " , " + thirdPartyEventState.value());
                // 清空状态
                thirdPartyEventState.clear();
            } else {
                // 更新状态
                appEventState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
            }
        }
    
        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if(appEventState.value() != null) {
                out.collect("对账成功：" + appEventState.value() + " , " + value);
                appEventState.clear();
            } else {
                thirdPartyEventState.update(value);
                ctx.timerService().registerEventTimeTimer(value.f3 + 2000L);
            }
        }
    
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if(appEventState.value() != null) {
                out.collect("对账失败：" + appEventState.value() + " , " + "第三方支付平台数据未到");
            }
            if(thirdPartyEventState.value() != null) {
                out.collect("对账失败：" + thirdPartyEventState.value() + " , " + "app数据未到");
            }
            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}
