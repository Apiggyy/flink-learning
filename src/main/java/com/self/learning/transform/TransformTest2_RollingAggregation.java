package com.self.learning.transform;

import com.self.learning.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\project\\flink-learning\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            SensorReading sensorReading = new SensorReading();
            sensorReading.setId(fields[0]);
            sensorReading.setTimestamp(Long.parseLong(fields[1]));
            sensorReading.setTemperature(Double.parseDouble(fields[2]));
            return sensorReading;
        });
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");
        resultStream.print();
        env.execute();
    }
}
