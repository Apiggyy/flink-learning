package com.self.learning.sink;

import com.self.learning.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.31.197:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "latest");

//        String inputPath = "D:\\project\\flink-learning\\src\\main\\resources\\sensor.txt";
//        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));

        DataStream<String> mapStream = inputStream.map((MapFunction<String, String>) lines -> {
            String[] fields = lines.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2])).toString();
        });
        mapStream.addSink(new FlinkKafkaProducer<>("sink_test", new SimpleStringSchema(), properties));
        env.execute();
    }
}
