package com.self.relearning.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

public class UdfTest_TableFunction {
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
        
        tableEnv.createTemporarySystemFunction("MySplit", MySplit.class);
        Table resultTable = tableEnv.sqlQuery("select user_name, url, word, length " +
                " from clickTable, LATERAL TABLE( MySplit(url) ) as T(word, length)");
        
        tableEnv.toDataStream(resultTable).print();
    
        env.execute();
    }
    
    public static class MySplit extends TableFunction<Tuple2<String, Integer>> {
        public void eval(String str) {
            String[] fields = str.split("\\?");
            for (String field : fields) {
                collect(Tuple2.of(field, field.length()));
            }
        }
    }
}
