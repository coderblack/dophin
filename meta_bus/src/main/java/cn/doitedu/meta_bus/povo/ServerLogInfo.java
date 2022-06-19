package cn.doitedu.meta_bus.povo;

public class ServerLogInfo {

    private String logServerName;
    private String logType;
    private String logDate;
    private long lineCnt;

    public ServerLogInfo() {
    }

    public ServerLogInfo(String logServerName, String logType, String logDate, long lineCnt) {
        this.logServerName = logServerName;
        this.logType = logType;
        this.logDate = logDate;
        this.lineCnt = lineCnt;
    }

    public String getLogServerName() {
        return logServerName;
    }

    public void setLogServerName(String logServerName) {
        this.logServerName = logServerName;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getLogDate() {
        return logDate;
    }

    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }

    public long getLineCnt() {
        return lineCnt;
    }

    public void setLineCnt(long lineCnt) {
        this.lineCnt = lineCnt;
    }

    @Override
    public String toString() {
        return "ServerLogInfo{" +
                "logServerName='" + logServerName + '\'' +
                ", logType='" + logType + '\'' +
                ", logDate='" + logDate + '\'' +
                ", lineCnt=" + lineCnt +
                '}';
    }
}
