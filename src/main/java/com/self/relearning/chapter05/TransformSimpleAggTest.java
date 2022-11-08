package com.self.relearning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("Bob", "./home", 1000L),
                new Event("Mary", "./prod?id=88", 2500L),
                new Event("Mary", "./cart", 2000L),
                new Event("Mary", "./cart", 4200L),
                new Event("Alice", "./home", 3000L),
                new Event("Alice", "./prod?id=88", 2500L),
                new Event("Bob", "./cart", 3500L),
                new Event("Bob", "./prod?id=3", 3600L),
                new Event("Bob", "./prod?id=1", 3300L),
                new Event("Bob", "./prod?id=2", 3400L)
        );
//        dataStream.keyBy(Event::getUser).max("timestamp").print("max");
        dataStream.keyBy(Event::getUser).maxBy("timestamp").print("maxBy");
        env.execute();
    }
}
