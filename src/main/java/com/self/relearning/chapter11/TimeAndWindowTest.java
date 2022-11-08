package com.self.relearning.chapter11;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // 1. 在创建表的DDL中直接定义时间属性
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT, " +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME( ts / 1000 ) ), " +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                "  'connector' = 'filesystem', " +
                "  'path' = 'input/clicks.txt', " +
                "  'format' = 'csv' " +
                ")";
        tableEnv.executeSql(createDDL);
        
        // 2.在流转换成Table的时候定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        Table clickTable = tableEnv.fromDataStream(
                clickStream,
                $("user"),
                $("url"),
                $("timestamp").as("ts"),
                $("et").rowtime()
        );
        clickTable.printSchema();
        
        // 聚合查询转换
        // 1. 分组聚合
        Table aggTable = tableEnv.sqlQuery("select user_name,count(url) from clickTable group by user_name");
    
        
        // 2.分组窗口聚合（老版本）
        Table groupWindowResultTable = tableEnv.sqlQuery(
                        "SELECT " +
                        "   user_name, count(*) as cnt, " +
                        "   TUMBLE_END(et, INTERVAL '10' SECOND) " +
                        " FROM clickTable " +
                        "GROUP BY " +
                        "   user_name, " +
                        "   TUMBLE(et, INTERVAL '10' SECOND)"
        );
        
        // 3. 窗口聚合
        // 3.1 滚动窗口
        Table tumbleWindowResultTable = tableEnv.sqlQuery("SELECT " +
                "    user_name, count(*) as cnt, " +
                "    window_end as entT " +
                "FROM TABLE (" +
                "    TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND) " +
                ") " +
                "GROUP BY " +
                "    user_name, window_start, window_end"
        );
    
        // 3.2 滑动窗口
        Table hopWindowResultTable = tableEnv.sqlQuery("SELECT " +
                "    user_name, count(*) as cnt, " +
                "    window_end as entT " +
                "FROM TABLE (" +
                "    HOP(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
                ") " +
                "GROUP BY " +
                "    user_name, window_start, window_end"
        );
        
        // 3.3 累计窗口
        Table cumulateWindowResultTable = tableEnv.sqlQuery("SELECT " +
                "    user_name, count(*) as cnt, " +
                "    window_end as entT " +
                "FROM TABLE (" +
                "    CUMULATE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '5' SECOND, INTERVAL '10' SECOND) " +
                ") " +
                "GROUP BY " +
                "    user_name, window_start, window_end"
        );
        
        // 4. 开窗聚合(OVER)
        Table overWindowResultTable = tableEnv.sqlQuery("SELECT" +
                "   user_name, " +
                "   AVG(ts) OVER (PARTITION BY user_name ORDER BY et ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ) as avg_ts " +
                " FROM clickTable");

//        tableEnv.toChangelogStream(aggTable).print("agg");
//        tableEnv.toChangelogStream(groupWindowResultTable).print("group window");
//        tableEnv.toChangelogStream(tumbleWindowResultTable).print("tumble window");
//        tableEnv.toChangelogStream(hopWindowResultTable).print("hop window");
//        tableEnv.toChangelogStream(cumulateWindowResultTable).print("cumulate window");
        tableEnv.toChangelogStream(overWindowResultTable).print("over window");
    
        env.execute();
    }
}
