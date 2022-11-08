package com.self.learning.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;


public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {

        /* 命令
        kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

        kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor

        kafka-topics.sh --delete --topic sensor --bootstrap-server localhost:9092

        kafka-topics.sh --describe --topic sensor --bootstrap-server localhost:9092

        kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.31.197:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("auto.offset.reset", "latest");
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties));
        dataStream.print();
        env.execute();
    }
}
