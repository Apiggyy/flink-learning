package com.self.relearning.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("Bob", "./home", 1000L),
                new Event("Mary", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 1000L),
                new Event("Mary", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 1000L),
                new Event("Mary", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );
        SingleOutputStreamOperator<Integer> result = dataStream.map(new RichMapper());
        result.print();
        env.execute();
    }

    public static class RichMapper extends RichMapFunction<Event, Integer> {
        @Override
        public Integer map(Event event) throws Exception {
            return event.getUrl().length();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用: " + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用: " + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
        }
    }
}
