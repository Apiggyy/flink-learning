package com.self.learning.transform;

import com.self.learning.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest5_RichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        String inputPath = "D:\\project\\flink-learning\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> mapStream = inputStream.map((MapFunction<String, SensorReading>) lines -> {
            String[] fields = lines.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        DataStream<Tuple2<String, Integer>> resultStream = mapStream.map(new RichMapFunction<SensorReading, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(SensorReading sensorReading) {
                return new Tuple2<>(sensorReading.getId(), this.getRuntimeContext().getIndexOfThisSubtask());
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("open");
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                System.out.println("close");
                super.close();
            }
        });
        resultStream.print();
        env.execute();
    }
}
