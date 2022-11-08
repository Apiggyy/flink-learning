package com.self.relearning.chapter08;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
    
        OutputTag<Tuple3<String, String, Long>> maryTag = new OutputTag<Tuple3<String, String, Long>>("Mary"){};
        OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob"){};
        SingleOutputStreamOperator<Event> processStream = dataStream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.getUser().equals("Mary")) {
                    ctx.output(maryTag, Tuple3.of(value.getUser(), value.getUrl(), value.getTimestamp()));
                } else if (value.getUser().equals("Bob")) {
                    ctx.output(BobTag, Tuple3.of(value.getUser(), value.getUrl(), value.getTimestamp()));
                } else {
                    out.collect(value);
                }
            }
        });
        processStream.print("else");
        processStream.getSideOutput(maryTag).print("maryTag");
        processStream.getSideOutput(BobTag).print("BobTag");
        env.execute();
    }
}
