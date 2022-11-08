package com.self.relearning.chapter08;

import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                Tuple2.of("Alice", 3500L),
                Tuple2.of("Mary", 2000L),
                Tuple2.of("Bob", 20000L),
                Tuple2.of("Alice", 35000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1)
        );
    
        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Bob", "./home", 1000L),
                new Event("Alice", "./home", 3000L),
                new Event("Alice", "./prod?id=88", 2500L),
                new Event("Mary", "./prod?id=88", 2500L),
                new Event("Mary", "./cart", 2000L),
                new Event("Mary", "./cart", 4200L),
                new Event("Bob", "./cart", 3500L),
                new Event("Alice", "./prod?id=88", 25000L),
                new Event("Bob", "./prod?id=3", 36000L),
                new Event("Bob", "./prod?id=1", 33000L),
                new Event("Bob", "./prod?id=2", 34000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
        );
    
        SingleOutputStreamOperator<String> result =
                orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(Event::getUser))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right + " => " + left);
                    }
                });
        result.print();
        env.execute();
    }
}
