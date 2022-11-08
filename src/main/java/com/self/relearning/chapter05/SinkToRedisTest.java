package com.self.relearning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> dataStream = env.addSource(new ClickSource());
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.31.197")
                .build();
        dataStream.addSink(new RedisSink<>(config, new RedisMapper<Event>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET, "clicks");
            }

            @Override
            public String getKeyFromData(Event data) {
                return data.getUser();
            }

            @Override
            public String getValueFromData(Event data) {
                return data.getUrl();
            }
        }));
        env.execute();
    }
}
