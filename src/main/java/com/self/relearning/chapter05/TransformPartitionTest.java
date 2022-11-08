package com.self.relearning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformPartitionTest {
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
                new Event("Alice", "./prod?id=88", 2500L)
                );
        // 1.随机分区
//        dataStream.shuffle().print().setParallelism(4);

        // 2.轮询分区
//        dataStream.rebalance().print().setParallelism(4);

        // 3.重缩放分区
//        env.addSource(new RichParallelSourceFunction<Integer>() {
//            @Override
//            public void run(SourceContext<Integer> ctx) throws Exception {
//                for (int i = 1; i <= 8; i++) {
//                    int index = this.getRuntimeContext().getIndexOfThisSubtask();
//                    if (i % 2 == index) {
//                        ctx.collect(i);
//                    }
//                }
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        }).setParallelism(2).rescale().print().setParallelism(4);

        // 4.广播
//        dataStream.broadcast().print().setParallelism(4);

        // 5.全局分区
//        dataStream.global().print().setParallelism(4);

        // 6.自定义重分区
        env.fromElements(1,2,3,4,5,6,7,8)
                .partitionCustom((key, numPartitions) -> key % 2, value -> value)
                .print()
                .setParallelism(4);

        env.execute();
    }
}
