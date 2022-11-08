package com.self.relearning.chapter07;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        dataStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                if (value.getUser().equals("Mary")) {
                    out.collect(value.getUser() + " clicks " + value.getUrl());
                } else if (value.getUser().equals("Bob")) {
                    out.collect(value.getUser());
                    out.collect(value.getUser());
                } else {
                    out.collect(value.getUser());
                }
                out.collect(value.toString());
                System.out.println("timestamp: " + ctx.timestamp());
                System.out.println("watermark: " + ctx.timerService().currentWatermark());
            }
        }).print();
        env.execute();
    }
}
