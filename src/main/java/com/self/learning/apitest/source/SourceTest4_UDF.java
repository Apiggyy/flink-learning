package com.self.learning.apitest.source;

import com.self.learning.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());
        dataStream.print();
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();
            Double randVal = 60 + random.nextGaussian() * 20;
            Map<String, Double> sensorTempMap = new HashMap<>();
            IntStream.rangeClosed(1, 10).forEach(x -> sensorTempMap.put("sensor_" + x, randVal));
            while (running) {
                for (String sensorId : sensorTempMap.keySet()) {
                    double newVal = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newVal);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newVal));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
