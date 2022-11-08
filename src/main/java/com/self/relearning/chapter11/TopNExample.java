package com.self.relearning.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNExample {
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
        
        // 普通Top N，选取当前所有用户中浏览量最大的2个
        String sql = "select t.user_name,t.cnt,t.rn from (\n" +
                "\tselect t.*,row_number() over(order by cnt desc) as rn from (\n" +
                "\t\tselect user_name,count(*) as cnt from clickTable group by user_name\n" +
                "\t) t\n" +
                ") t where t.rn <= 2";
        Table topNResultTable = tableEnv.sqlQuery(sql);
//        tableEnv.toChangelogStream(topNResultTable).print("TopN");
        
        
        // 窗口Top N 统计一段时间内的（前2名）活跃用户
        sql = "select\n" +
                "   user_name\n" +
                "  ,cnt\n" +
                "  ,rn\n" +
                "  ,window_start\n" +
                "  ,window_end\n" +
                "from (\n" +
                "\tselect\n" +
                "\t\ta.*,row_number() over(partition by window_start,window_end order by cnt desc) as rn\n" +
                "\tfrom (\n" +
                "\t\tselect\n" +
                "\t\t\t  user_name\n" +
                "\t\t\t, window_start\n" +
                "\t\t\t, window_end\n" +
                "\t\t\t, count(*) as cnt\n" +
                "\t\tfrom TABLE(TUMBLE(TABLE clickTable, DESCRIPTOR(et), INTERVAL '10' SECOND))\n" +
                "\t\tgroup by\n" +
                "\t\t\t  user_name,window_start,window_end\n" +
                "\t) a\n" +
                ") a where a.rn <= 2";
    
        Table windowTopNResultTable = tableEnv.sqlQuery(sql);
        tableEnv.toDataStream(windowTopNResultTable).print("windowTopN");
        env.execute();
    }
}
