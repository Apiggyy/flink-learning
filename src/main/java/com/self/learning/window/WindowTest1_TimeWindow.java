package com.self.learning.window;

import com.self.learning.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {

        //滑动窗口和滚动窗口的函数名是完全一致的，只是在传参数时需要传入两个参数，一个是window_size，一个是sliding_size。
        // CountWindow的window_size指的是相同Key的元素的个数，不是输入的所有元素的总数。

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 1.增量聚合函数
        DataStream<Integer> resultStream1 = dataStream
                .keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
//                .timeWindow(Time.seconds(15)) // 滚动窗口
//                .countWindow(5)
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        // 2.全窗口函数
        DataStream<Tuple3<String, Long, Integer>> resultStream2 = dataStream
                .keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .apply((s, window, input, out) -> {
                    int count = IteratorUtils.toList(input.iterator()).size();
                    out.collect(new Tuple3<>(s, window.getEnd(), count));
                });

        // 3. 其他可选API
        OutputTag<SensorReading> lateTag = new OutputTag<>("late");
        SingleOutputStreamOperator<SensorReading> sumStream = dataStream
                .keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .sum("temperature");

        sumStream.getSideOutput(lateTag).print("late");
//        resultStream1.print();
//        resultStream2.print();
        sumStream.print();
        env.execute();
    }
}
