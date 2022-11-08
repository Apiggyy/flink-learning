package com.self.relearning.chapter06;

import java.sql.Timestamp;

public class UrlCountView {
    private String url;
    private Long count;
    private Long startTime;
    private Long endTime;

    public UrlCountView() {
    }

    public UrlCountView(String url, Long count, Long startTime, Long endTime) {
        this.url = url;
        this.count = count;
        this.startTime = startTime;
        this.endTime = endTime;
    }
    
    public String getUrl() {
        return url;
    }
    
    public void setUrl(String url) {
        this.url = url;
    }
    
    public Long getCount() {
        return count;
    }
    
    public void setCount(Long count) {
        this.count = count;
    }
    
    public Long getStartTime() {
        return startTime;
    }
    
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
    
    public Long getEndTime() {
        return endTime;
    }
    
    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }
    
    @Override
    public String toString() {
        return "UrlCountView{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", startTime=" + new Timestamp(startTime) +
                ", endTime=" + new Timestamp(endTime) +
                '}';
    }
}
