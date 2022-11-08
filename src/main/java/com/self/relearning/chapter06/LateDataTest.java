package com.self.relearning.chapter06;

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
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置水位线时间间隔
        env.getConfig().setAutoWatermarkInterval(100);
//        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
        SingleOutputStreamOperator<Event> dataStream = env.socketTextStream("192.168.31.10", 7777)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.parseLong(fields[2].trim()));
                })
//               .assignTimestampsAndWatermarks(
//                WatermarkStrategy.<Event>forMonotonousTimestamps()
//               .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
//               )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );

        dataStream.print("data");

        OutputTag<Event> late = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<UrlCountView> result = dataStream.keyBy(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1)) //也是基于事件时间
                .sideOutputLateData(late)
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Event value, Long accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                }, new ProcessWindowFunction<Long, UrlCountView, String, TimeWindow>() {
                    @Override
                    public void process(String url, Context context, Iterable<Long> elements, Collector<UrlCountView> out) throws Exception {
                        Long startTime = context.window().getStart();
                        Long endTime = context.window().getEnd();
                        Long count = elements.iterator().next();
                        out.collect(new UrlCountView(url, count, startTime, endTime));
                    }
                });
        result.print("result");
        result.getSideOutput(late).print("late");
        env.execute();
    }
}
