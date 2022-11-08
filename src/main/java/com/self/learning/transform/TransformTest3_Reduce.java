package com.self.learning.transform;

import com.self.learning.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\project\\flink-learning\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> mapStream = inputStream.map((MapFunction<String, SensorReading>) lines -> {
            String[] fields = lines.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        KeyedStream<SensorReading, String> keyedStream = mapStream.keyBy(SensorReading::getId);
        DataStream<SensorReading> reduceStream = keyedStream.reduce((s1, s2) -> new SensorReading(s1.getId(), s2.getTimestamp(), Math.max(s1.getTemperature(), s2.getTemperature())));
        reduceStream.print();
        env.execute();
    }
}
