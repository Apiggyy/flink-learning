package com.self.relearning.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
//        DataStreamSource<Event> ds = env.addSource(new ClickSource());
        DataStreamSource<Integer> ds = env.addSource(new ParallelCustomSource()).setParallelism(2);
        ds.print();
        env.execute();
    }

    private static class ParallelCustomSource implements ParallelSourceFunction<Integer> {

        private boolean running = true;
        private final Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while(running) {
                ctx.collect(random.nextInt());
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
