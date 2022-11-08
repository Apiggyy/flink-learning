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

public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.读取数据，转换为DataStream
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        
        // 3.将DataStream转换成table
        Table eventTable = tableEnv.fromDataStream(dataStream);
        
        // 4.直接写SQL进行转换
        Table resultTable1 = tableEnv.sqlQuery("select user, url,`timestamp` from " + eventTable);
        
        // 5.基于Table直接转换
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Alice"));
    
        // 6.转换成流打印输出
        tableEnv.toDataStream(resultTable1).print("result1");
//        tableEnv.toDataStream(resultTable2).print("result2");
    
        tableEnv.createTemporaryView("eventTable", eventTable);
        Table aggTable = tableEnv.sqlQuery("select user, count(*) as cnt from eventTable group by user");
        tableEnv.toChangelogStream(aggTable).print("agg");
    
        env.execute();
    }
}
