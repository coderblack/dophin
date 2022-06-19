package cn.doitedu.log_collect.flume;

public class LogBean {

    private long timestamp;

    public LogBean() {
    }

    public LogBean(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
