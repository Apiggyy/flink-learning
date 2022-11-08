package com.self.learning.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceTest2_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("D:\\project\\flink-learning\\src\\main\\resources\\sensor.txt");
        dataStream.print();
        env.execute();
    }
}
