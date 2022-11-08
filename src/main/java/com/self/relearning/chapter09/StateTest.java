package com.self.relearning.chapter09;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream
                .keyBy(Event::getUser)
                .flatMap(new MyFlatMap())
                .print();
                
        env.execute();
    }
    
    public static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        private ValueState<Event> myValueState;
        private ListState<Event> myListState;
        private MapState<String, Long> myMapState;
        private ReducingState<Event> myReducingState;
        private AggregatingState<Event, String> myAggregatingState;
    
        @Override
        public void open(Configuration parameters) throws Exception {
            
            // 配置状态的TTL  只支持处理时间语义
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            ValueStateDescriptor<Event> valueStateDescriptor = new ValueStateDescriptor<>("myValueState", Event.class);
            
            valueStateDescriptor.enableTimeToLive(ttlConfig);
            
            this.myValueState = getRuntimeContext().getState(
                    valueStateDescriptor
            );
            
            
            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("myListState", Event.class)
            );
            myMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Long>("myMapState", String.class, Long.class)
            );
            myReducingState = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<Event>("myReducingState", new ReduceFunction<Event>() {
                        @Override
                        public Event reduce(Event value1, Event value2) throws Exception {
                            return new Event(value1.getUser(), value1.getUrl(), value2.getTimestamp());
                        }
                    }, Event.class)
            );
    
            myAggregatingState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Event, Long, String>("myAggregatingState", new AggregateFunction<Event, Long, String>() {
                        @Override
                        public Long createAccumulator() {
                            return 0L;
                        }
    
                        @Override
                        public Long add(Event value, Long accumulator) {
                            return accumulator + 1;
                        }
    
                        @Override
                        public String getResult(Long accumulator) {
                            return "count: " + accumulator;
                        }
    
                        @Override
                        public Long merge(Long a, Long b) {
                            return a + b;
                        }
                    }, Long.class)
            );
        }
    
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
//            System.out.println(myValueState.value());
            myValueState.update(value);
//            System.out.println("my value: " + myValueState.value());
            
            myListState.add(value);
            System.out.println("my list state: " + myListState.get());
            
            myMapState.put(value.getUser(), myMapState.get(value.getUser()) == null ? 1 : myMapState.get(value.getUser()) + 1);
            System.out.println("my map value: " + value.getUser() + "->" + myMapState.get(value.getUser()));
    
            myAggregatingState.add(value);
            System.out.println("my aggregating value: " + myAggregatingState.get());
            
            myReducingState.add(value);
            System.out.println("my reducing value: " + myReducingState.get());
        }
    }
}
