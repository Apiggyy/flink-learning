package com.self.relearning.chapter12;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class LoginFailDetectExample {
    
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 1.获取登录数据流
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.fromElements(
                new LoginEvent("user_1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user_1", "192.168.0.2", "fail", 3000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 4000L),
                new LoginEvent("user_1", "171.56.23.10", "fail", 5000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 7000L),
                new LoginEvent("user_2", "192.168.1.29", "fail", 8000L),
                new LoginEvent("user_2", "192.168.1.29", "success", 6000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((element, recordTimestamp) -> element.timestamp)
        );
        
        // 2.定义模式，连续三次登录失败
//        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent value) throws Exception {
//                        return value.eventType.equals("fail");
//                    }
//                })
//                .next("second")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent value) throws Exception {
//                        return value.eventType.equals("fail");
//                    }
//                })
//                .next("third")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent value) throws Exception {
//                        return value.eventType.equals("fail");
//                    }
//                });
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return value.eventType.equals("fail");
                    }
                }).times(3).consecutive();
    
        // 3.将模式应用到数据流上，检测复杂事件
        PatternStream<LoginEvent> patternStream =
                CEP.pattern(loginEventStream.keyBy(data -> data.userId), pattern);
        
        // 4. 将检测到的复杂事件提取出来，进行处理得到报警信息输出
        SingleOutputStreamOperator<String> warningStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
//                LoginEvent first = pattern.get("first").get(0);
//                LoginEvent second = pattern.get("second").get(0);
//                LoginEvent third = pattern.get("third").get(0);
//
//                return first.userId + "连续三次登录失败，登录时间：" +
//                        first.timestamp + ", " +
//                        second.timestamp + ", " +
//                        third.timestamp;
                return pattern.get("first").toString();
            }
        });
        
        warningStream.print();
        
        env.execute();
    }
    
}
