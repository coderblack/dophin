package cn.doitedu.meta_bus.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcUtil {

    public static Connection getConn() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:mysql://doit01:3306/abc", "root", "ABC123.abc123");
        return conn;
    }

}
