package com.self.relearning.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L),
                Tuple2.of("b", 3000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1)
        );
    
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream2 = env.fromElements(
                Tuple2.of("a", 3000),
                Tuple2.of("a", 4000),
                Tuple2.of("b", 4000),
                Tuple2.of("b", 5500)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((element, recordTimestamp) -> element.f1)
        );
    
        DataStream<String> dataStream = stream1.join(stream2)
                .where(data -> data.f0)
                .equalTo(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((first, second) -> first + " -> " + second);
        dataStream.print();
        env.execute();
    }
}
