package com.self.relearning.chapter06;

import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WaterMarkTest {
    /**
     * Alice, ./home, 1000
     * Alice, ./cart, 2000
     * Alice, ./prod?id=100, 10000
     * Alice, ./prod?id=200, 8000
     * Alice, ./prod?id=300, 15000
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.socketTextStream("192.168.31.10", 7777)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new Event(fields[0].trim(), fields[1].trim(), Long.parseLong(fields[2].trim()));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Event, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        long count = elements.spliterator().getExactSizeIfKnown();
                        long watermark = context.currentWatermark();
                        System.out.println("watermark: " + watermark);
                        out.collect("窗口【" + start + "~" + end + "】中共有" + count + "元素，窗口闭合时，水位线处于：" + watermark);
                    }
                })
                .print();
        env.execute();
    }
}
