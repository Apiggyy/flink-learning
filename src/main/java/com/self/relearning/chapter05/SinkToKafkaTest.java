package com.self.relearning.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.31.197:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> dataStream = env.addSource(new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), properties));
        SingleOutputStreamOperator<String> mapStream = dataStream.map(data -> {
            String[] fields = data.split(",");
            return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
        });
        mapStream.addSink(new FlinkKafkaProducer<String>("192.168.31.197:9092", "event", new SimpleStringSchema()));
        env.execute();
    }
}