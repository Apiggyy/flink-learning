package com.self.relearning.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("Bob", "./home", 1000L),
                new Event("Alice", "./home", 3000L),
                new Event("Alice", "./prod?id=88", 2500L),
                new Event("Mary", "./prod?id=88", 2500L),
                new Event("Mary", "./cart", 2000L),
                new Event("Mary", "./cart", 4200L),
                new Event("Bob", "./cart", 3500L),
                new Event("Alice", "./prod?id=88", 2500L)
        );

        StreamingFileSink<String> streamingFileSink = StreamingFileSink
                .<String>forRowFormat(new Path("output"), new SimpleStringEncoder<>("utf-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .build()
                )
                .build();
        dataStream
                .map(Event::toString)
                .addSink(streamingFileSink);
        env.execute();
    }
}
