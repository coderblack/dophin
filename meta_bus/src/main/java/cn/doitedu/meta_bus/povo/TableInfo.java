package cn.doitedu.meta_bus.povo;

public class TableInfo {

    private String lastUpdateTime;
    private String maxFileSize;
    private String totalFileSize;
    private String tableName;
    private String dbName;
    private String minFileSize;
    private String lastAccessTime;
    private String totalNumberFiles;

    public TableInfo() {
    }

    public TableInfo(String lastUpdateTime, String maxFileSize, String totalFileSize, String tableName, String dbName, String minFileSize, String lastAccessTime, String totalNumberFiles) {
        this.lastUpdateTime = lastUpdateTime;
        this.maxFileSize = maxFileSize;
        this.totalFileSize = totalFileSize;
        this.tableName = tableName;
        this.dbName = dbName;
        this.minFileSize = minFileSize;
        this.lastAccessTime = lastAccessTime;
        this.totalNumberFiles = totalNumberFiles;
    }


    public String getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(String lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public String getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(String maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public String getTotalFileSize() {
        return totalFileSize;
    }

    public void setTotalFileSize(String totalFileSize) {
        this.totalFileSize = totalFileSize;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getMinFileSize() {
        return minFileSize;
    }

    public void setMinFileSize(String minFileSize) {
        this.minFileSize = minFileSize;
    }

    public String getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(String lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public String getTotalNumberFiles() {
        return totalNumberFiles;
    }

    public void setTotalNumberFiles(String totalNumberFiles) {
        this.totalNumberFiles = totalNumberFiles;
    }

    @Override
    public String toString() {
        return "TableInfo{" +
                "lastUpdateTime='" + lastUpdateTime + '\'' +
                ", maxFileSize='" + maxFileSize + '\'' +
                ", totalFileSize='" + totalFileSize + '\'' +
                ", tableName='" + tableName + '\'' +
                ", dbName='" + dbName + '\'' +
                ", minFileSize='" + minFileSize + '\'' +
                ", lastAccessTime='" + lastAccessTime + '\'' +
                ", totalNumberFiles='" + totalNumberFiles + '\'' +
                '}';
    }
}
