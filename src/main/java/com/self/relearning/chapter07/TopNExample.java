package com.self.relearning.chapter07;

import com.self.relearning.chapter05.ClickSource;
import com.self.relearning.chapter05.Event;
import com.self.relearning.chapter06.UrlCountView;
import com.self.relearning.chapter06.UrlCountViewExample;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

public class TopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTimestamp())
                );
        SingleOutputStreamOperator<UrlCountView> urlCountStream = dataStream
                .keyBy(Event::getUrl)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountViewExample.UrlCountViewAgg(), new UrlCountViewExample.UrlCountViewResult());
        urlCountStream.print("url count");
        urlCountStream
                .keyBy(UrlCountView::getEndTime)
                .process(new TopNProcessResult(2))
                .print();
                
        
        env.execute();
    }
    
    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlCountView, String> {
    
        private Integer n;
        
        // 定义列表状态
        private ListState<UrlCountView> urlCountViewListState;
    
        @Override
        public void open(Configuration parameters) throws Exception {
            this.urlCountViewListState = getRuntimeContext()
                    .getListState(new ListStateDescriptor<UrlCountView>("url-count-view-list", Types.POJO(UrlCountView.class)));
        }
    
        public TopNProcessResult(Integer n) {
            this.n = n;
        }
        
        @Override
        public void processElement(UrlCountView value, Context ctx, Collector<String> out) throws Exception {
            urlCountViewListState.add(value);
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }
    
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlCountView> urlCountViewList = new ArrayList<>();
            for(UrlCountView urlCountView : this.urlCountViewListState.get()) {
                urlCountViewList.add(urlCountView);
            }
            urlCountViewList.sort((o1, o2) -> o2.getCount().intValue() - o1.getCount().intValue());
            StringBuilder result = new StringBuilder();
            result.append("--------------------\n");
            result.append("窗口结束时间:" + new Timestamp(ctx.getCurrentKey()) + "\n");
            for (int i = 0; i < 2; i++) {
                UrlCountView urlCountView = urlCountViewList.get(i);
                String info = "No." + (i + 1) + " "
                        + "Url: " + urlCountView.getUrl() + " "
                        + "访问量: " + urlCountView.getCount() + "\n";
                result.append(info);
            }
            result.append("--------------------\n");
            out.collect(result.toString());
        }
    }
}
