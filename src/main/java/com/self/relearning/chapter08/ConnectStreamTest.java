package com.self.relearning.chapter08;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> connStream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Long>  connStream2 = env.fromElements(1L, 2L, 3L);
    
        ConnectedStreams<Integer, Long> connStream3 = connStream1.connect(connStream2);
        connStream3.map(new CoMapFunction<Integer, Long, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "Integer: " + value;
            }
        
            @Override
            public String map2(Long value) throws Exception {
                return "Long: " + value;
            }
        }).print();
        env.execute();
    
    }
}
