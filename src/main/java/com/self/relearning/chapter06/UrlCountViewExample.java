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

import java.time.Duration;

public class UrlCountViewExample {
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
        dataStream.keyBy(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UrlCountViewAgg(), new UrlCountViewResult())
                .print();
        env.execute();
    }
    
    public static class UrlCountViewAgg implements AggregateFunction<Event, Long, Long> {
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
    }
    
    public static class UrlCountViewResult extends ProcessWindowFunction<Long, UrlCountView, String, TimeWindow> {
    
        @Override
        public void process(String url, Context context, Iterable<Long> elements, Collector<UrlCountView> out) throws Exception {
            Long startTime = context.window().getStart();
            Long endTime = context.window().getEnd();
            Long count = elements.iterator().next();
            out.collect(new UrlCountView(url, count, startTime, endTime));
        }
    }
}
