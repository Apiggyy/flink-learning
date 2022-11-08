package com.self.relearning.chapter08;

import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class UnionStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream1 = env.socketTextStream("192.168.31.10", 7777)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new Event(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        SingleOutputStreamOperator<Event> dataStream2 = env.socketTextStream("192.168.31.10", 8888)
                .map(value -> {
                    String[] fields = value.split(",");
                    return new Event(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
    
        dataStream1
                .union(dataStream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("当前水位线： " + ctx.timerService().currentWatermark());
                    }
                })
                .print();
        env.execute();
    }
}
