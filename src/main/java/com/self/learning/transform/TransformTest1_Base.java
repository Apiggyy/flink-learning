package com.self.learning.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest1_Base {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        String inputPath = "D:\\project\\flink-learning\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        // map
        DataStream<Integer> mapStream = inputStream.map((MapFunction<String, Integer>) String::length);
        // flatmap
        DataStream<String> flatMapStream = inputStream.flatMap((FlatMapFunction<String, String>) (s, c) -> {
            for (String s1 : s.split(",")) {
                c.collect(s1);
            }
        }).returns(Types.STRING);

        //filter
        DataStream<String> filterStream = inputStream.filter((FilterFunction<String>) s -> s.startsWith("sensor_1"));
        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");
        env.execute();
    }
}
