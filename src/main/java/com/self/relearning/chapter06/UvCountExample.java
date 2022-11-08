package com.self.relearning.chapter06;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class UvCountExample {
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
        //使用ProcessWindowFunction计算UV
        dataStream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<Event, HashSet<String>, Long>() {
                    @Override
                    public HashSet<String> createAccumulator() {
                        return new HashSet<>();
                    }

                    @Override
                    public HashSet<String> add(Event value, HashSet<String> accumulator) {
                        accumulator.add(value.getUser());
                        return accumulator;
                    }

                    @Override
                    public Long getResult(HashSet<String> accumulator) {
                        return (long) accumulator.size();
                    }

                    @Override
                    public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
                        Long uv = elements.iterator().next();
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        out.collect("窗口周期：" + new Timestamp(start) + "~" + new Timestamp(end) + ",UV值为：" + uv);
                    }
                }).print();
                
        env.execute();
    }
}
