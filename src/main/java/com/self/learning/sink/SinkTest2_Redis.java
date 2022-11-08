package com.self.learning.sink;

import com.self.learning.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "D:\\project\\flink-learning\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> mapStream = inputStream.map((MapFunction<String, SensorReading>) lines -> {
            String[] fields = lines.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig
                .Builder()
                .setHost("192.168.31.197")
                .setPort(6379)
                .build();

        mapStream.addSink(new RedisSink<>(config, new RedisMapper<SensorReading>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature");
            }

            @Override
            public String getKeyFromData(SensorReading data) {
                return data.getId();
            }

            @Override
            public String getValueFromData(SensorReading data) {
                return data.getTemperature().toString();
            }
        }));
        env.execute();
    }
}
