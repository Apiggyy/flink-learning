package com.self.relearning.chapter09;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;


/**
 * 讲解AggregatingState使用
 */
public class AverageTimestampExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.print("input");
        dataStream.keyBy(Event::getUser)
                .flatMap(new AvgTsResult(5))
                .print();
        env.execute();
    }
    
    public static class AvgTsResult extends RichFlatMapFunction<Event, String> {
        
        private final long n;
        private ValueState<Long> countState;
        private AggregatingState<Event, Long> aggTsState;
        
        public AvgTsResult(long n) {
            this.n = n;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("count", Long.class)
            );
            aggTsState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>(
                            "avgTs",
                            new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                                @Override
                                public Tuple2<Long, Long> createAccumulator() {
                                    return Tuple2.of(0L, 0L);
                                }
                        
                                @Override
                                public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
                                    return Tuple2.of(accumulator.f0 + value.getTimestamp(), accumulator.f1 + 1);
                                }
                        
                                @Override
                                public Long getResult(Tuple2<Long, Long> accumulator) {
                                    return accumulator.f0 / accumulator.f1;
                                }
                        
                                @Override
                                public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                                    return null;
                                }
                            },
                            Types.TUPLE(Types.LONG, Types.LONG)
                    )
            );
        }
        
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            Long currentCount = countState.value();
            if(currentCount == null) {
                currentCount = 1L;
            } else {
                currentCount++;
            }
            countState.update(currentCount);
            aggTsState.add(value);
            if (currentCount == n) {
                out.collect(value.getUser() + "过去" + n + "次访问平均时间戳为: " + new Timestamp((Long)aggTsState.get()));
                aggTsState.clear();
                countState.clear();
            }
        }
    }
}
