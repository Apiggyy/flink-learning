package com.self.relearning.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class UdfTest_ScalaFunction {
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
        
        tableEnv.createTemporarySystemFunction("MyHash", MyHashFuction.class);
    
        Table resultTable = tableEnv.sqlQuery("select user_name, MyHash(user_name) from clickTable");
        tableEnv.toDataStream(resultTable).print();
        env.execute();
    }
    
    public static class MyHashFuction extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}
