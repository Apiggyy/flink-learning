package com.self.relearning.chapter06;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置水位线时间间隔
        env.getConfig().setAutoWatermarkInterval(100);
//        SingleOutputStreamOperator<Event> dataStream = env.fromElements(
//                new Event("Bob", "./home", 1000L),
//                new Event("Alice", "./home", 3000L),
//                new Event("Alice", "./prod?id=88", 2500L),
//                new Event("Mary", "./prod?id=88", 2500L),
//                new Event("Mary", "./cart", 2000L),
//                new Event("Mary", "./cart", 4200L),
//                new Event("Bob", "./cart", 3500L),
//                new Event("Alice", "./prod?id=88", 2500L)
//        )
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
//               .assignTimestampsAndWatermarks(
//                WatermarkStrategy.<Event>forMonotonousTimestamps()
//               .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
//               )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream
                .map(data -> Tuple2.of(data.getUser(), 1L))
                .returns(new TypeHint<Tuple2<String, Long>>() {
                })
                .keyBy(data -> data.f0)
//                .countWindow(10) //滚动计数窗口
//                .countWindow(10, 2) //滑动计数窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5))) //滚动事件时间窗口
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(30))) //滑动事件时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2)))  //事件时间会话窗口
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .print();

        env.execute();
    }
}
