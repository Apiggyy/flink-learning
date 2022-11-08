package com.self.relearning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("Bob", "./home", 1000L),
                new Event("Mary", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );
        SingleOutputStreamOperator<Event> filterStream = dataStream.filter(event -> event.getUser().equals("Mary"));
        filterStream.print();
        env.execute();
    }
}
