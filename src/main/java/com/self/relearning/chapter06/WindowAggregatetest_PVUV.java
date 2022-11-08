package com.self.relearning.chapter06;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;

public class WindowAggregatetest_PVUV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置水位线时间间隔
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
//               .assignTimestampsAndWatermarks(
//                WatermarkStrategy.<Event>forMonotonousTimestamps()
//               .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
//               )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.print("data");
        dataStream
                .keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
                    @Override
                    public Tuple2<Long, HashSet<String>> createAccumulator() {
                        return Tuple2.of(0L, new HashSet<>());
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> add(Event value, Tuple2<Long, HashSet<String>> accumulator) {
                        accumulator.f1.add(value.getUser());
                        return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
                    }

                    @Override
                    public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
                        return (double) accumulator.f0 / accumulator.f1.size();
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> a, Tuple2<Long, HashSet<String>> b) {
                        b.f1.addAll(a.f1);
                        return Tuple2.of(a.f0 + b.f0, b.f1);
                    }
                }).print();
        env.execute();
    }
}
