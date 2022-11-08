package com.self.relearning.chapter05;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("Bob", "./home", 1000L),
                new Event("Alice", "./home", 3000L),
                new Event("Alice", "./prod?id=88", 2500L),
                new Event("Mary", "./prod?id=88", 2500L),
                new Event("Mary", "./cart", 2000L),
                new Event("Mary", "./cart", 4200L),
                new Event("Bob", "./cart", 3500L),
                new Event("Alice", "./prod?id=88", 2500L),
                new Event("Bob", "./prod?id=3", 3600L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./prod?id=2", 3400L)
        );
        SingleOutputStreamOperator<Tuple2<String, Long>> clickStream = dataStream
                .map(event -> Tuple2.of(event.getUser(), 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(t -> t.f0)
                .reduce((v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1));
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickStream
                .keyBy(data -> "key")
                .reduce((v1, v2) -> v1.f1 > v2.f1 ? v1 : v2);
        result.print();
        env.execute();
    }
}
