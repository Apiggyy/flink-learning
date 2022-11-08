package com.self.relearning.chapter11;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CommonApiTest {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        
        // 1. 定义环境配置来创建表执行环境
        EnvironmentSettings settings1 = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings1);
    
        // 1.2 基于老版本planner进行批处理
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);
    
        // 1.3 基于blink版本planner进行批处理
//        EnvironmentSettings settings2 = EnvironmentSettings
//                .newInstance()
//                .inBatchMode()
//                .useBlinkPlanner()
//                .build();
//        TableEnvironment tableEnv1 = TableEnvironment.create(settings2);
        
        // 2. 创建表
        String createDDL = "CREATE TABLE clickTable (" +
                "user_name STRING," +
                "url STRING," +
                "ts BIGINT" +
                ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = 'input/clicks.txt'," +
                "  'format' = 'csv'" +
                ")";
        tableEnv.executeSql(createDDL);
        
        // 调用Table API进行表的查询转换
        Table clickTable = tableEnv.from("clickTable");
        Table resultTable = clickTable
                .where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));
        tableEnv.createTemporaryView("result2", resultTable);
        
        // 执行SQL进行表的查询转换
        Table resultTable2 = tableEnv.sqlQuery("select url, user_name from result2");
    
        // 创建一张输出的表
//        String createOutDDL = "CREATE TABLE outTable (" +
//                "url STRING," +
//                "user_name STRING" +
//                ") WITH (" +
//                "  'connector' = 'filesystem'," +
//                "  'path' = 'output'," +
//                "  'format' = 'csv'" +
//                ")";
//        tableEnv.executeSql(createOutDDL);
//        resultTable2.executeInsert("outTable");
    
        Table aggTable = tableEnv.sqlQuery("select user_name, count(*) from clickTable group by user_name");
    
        // 创建一张输出的表
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                "user_name STRING," +
                "cnt BIGINT" +
                ") WITH (" +
                "  'connector' = 'print')";
        tableEnv.executeSql(createPrintOutDDL);
        
//        resultTable2.executeInsert("printOutTable");
        aggTable.executeInsert("printOutTable");
    }
}
