package com.self.relearning.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class UdfTest_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND" +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'input/clicks.txt', " +
                " 'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createDDL);
    
        tableEnv.createTemporarySystemFunction("WeightedAverage", WeightedAverage.class);
        Table resultTable = tableEnv.sqlQuery("select user_name, WeightedAverage(ts, 1) from clickTable group by user_name");
        tableEnv.toChangelogStream(resultTable).print();
    
        env.execute();
    }
    
    public static class WeightedAvgAccumulator {
        public long sum = 0;
        public int count = 0;
    }
    
    public static class WeightedAverage extends AggregateFunction<Long, WeightedAvgAccumulator> {
        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if(accumulator.count == 0) {
                return null;
            } else {
                return accumulator.sum / accumulator.count;
            }
        }
    
        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }
        
        public void accumulate(WeightedAvgAccumulator accumulator, Long iValue, Integer weight) {
            accumulator.sum += iValue * weight;
            accumulator.count += 1;
        }
    }
}
