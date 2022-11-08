package com.self.relearning.chapter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("Bob", "./home", 1000L),
                new Event("Mary", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );
        SingleOutputStreamOperator<String> flatMapStream = dataStream.flatMap((FlatMapFunction<Event, String>) (event, collector) -> {
            collector.collect(event.getUser());
            collector.collect(event.getUrl());
            collector.collect(event.getTimestamp().toString());
        }).returns(Types.STRING);
        flatMapStream.print();
        env.execute();
    }
}
