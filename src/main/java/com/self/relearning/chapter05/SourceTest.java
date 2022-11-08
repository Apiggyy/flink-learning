package com.self.relearning.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        //1. 从文件读取
//        DataStreamSource<String> ds1 = env.readTextFile("input/clicks.txt");
//        //2. 从集合中读取
//        DataStreamSource<Integer> ds2 = env.fromCollection(Arrays.asList(10, 20, 30, 40, 50));
//
//        List<Event> events = new ArrayList<>();
//        events.add(new Event("Bob", "./home", 1000L));
//        events.add(new Event("Mary", "./cart", 2000L));
//        events.add(new Event("Alice", "./prod?id=100", 3000L));
//        DataStreamSource<Event> ds3 = env.fromCollection(events);
//
//        //3. 从元素读取
//        DataStreamSource<Event> ds4 = env.fromElements(
//                new Event("Bob", "./home", 1000L),
//                new Event("Mary", "./cart", 2000L),
//                new Event("Alice", "./prod?id=100", 3000L)
//        );
//
//        //4. 从socket文本流读取
//        DataStreamSource<String> ds5 = env.socketTextStream("wei", 7777);

        // 5.从kafka读取数据
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "consumer-wei");
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> ds6 = env.addSource(new FlinkKafkaConsumer<>("clicks", new SimpleStringSchema(), props));

//        ds1.print("file");
//        ds2.print("nums");
//        ds3.print("event");
//        ds4.print("element");
//        ds5.print("socket");
        ds6.print("kafka");
        env.execute();
    }
}
