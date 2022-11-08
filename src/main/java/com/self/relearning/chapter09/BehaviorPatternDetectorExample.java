package com.self.relearning.chapter09;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;


/**
 * 讲解 算子状态中的BroadcastState
 */
public class BehaviorPatternDetectorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Alice", "login"),
                new Action("Alice", "pay"),
                new Action("Carol", "login"),
                new Action("Carol", "browse"),
                new Action("Bob", "login"),
                new Action("Bob", "order")
        );
        
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );
        
        MapStateDescriptor<Void, Pattern> descriptor
                = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);
        
        actionStream.keyBy(Action::getUserId)
                .connect(broadcastStream)
                .process(new PatternDetector())
                .print();
        
        env.execute();
    }
    
    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        
        private ValueState<Action> preActionState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            preActionState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("pre-action", Types.POJO(Action.class))
            );
        }
        
        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            MapStateDescriptor<Void, Pattern> descriptor
                    = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
            ReadOnlyBroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(descriptor);
            
            Pattern pattern = bcState.get(null);
            Action preAction = preActionState.value();
            
            if (pattern != null && preAction != null) {
                if (preAction.getAction().equals(pattern.action1) && value.getAction().equals(pattern.action2)) {
                    out.collect(Tuple2.of(value.userId, pattern));
                }
            }
            preActionState.update(value);
        }
        
        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            MapStateDescriptor<Void, Pattern> descriptor
                    = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
            BroadcastState<Void, Pattern> bcState = ctx.getBroadcastState(descriptor);
            bcState.put(null, value);
        }
    }
    
    public static class Action {
        private String userId;
        private String action;
        
        public Action() {
        }
        
        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }
        
        public String getUserId() {
            return userId;
        }
        
        public void setUserId(String userId) {
            this.userId = userId;
        }
        
        public String getAction() {
            return action;
        }
        
        public void setAction(String action) {
            this.action = action;
        }
        
        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }
    
    public static class Pattern {
        private String action1;
        private String action2;
        
        public Pattern() {
        }
        
        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }
        
        public String getAction1() {
            return action1;
        }
        
        public void setAction1(String action1) {
            this.action1 = action1;
        }
        
        public String getAction2() {
            return action2;
        }
        
        public void setAction2(String action2) {
            this.action2 = action2;
        }
        
        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
