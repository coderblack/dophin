package cn.doitedu.meta_bus.povo;

public class LineData {
    private String date;
    private String host;
    private String logType;
    private int cnt;

    public LineData() {
    }

    public LineData(String date, String host, String logType, int cnt) {
        this.date = date;
        this.host = host;
        this.logType = logType;
        this.cnt = cnt;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public int getCnt() {
        return cnt;
    }

    public void setCnt(int cnt) {
        this.cnt = cnt;
    }
}
