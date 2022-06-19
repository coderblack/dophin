package cn.doitedu.meta_bus.controller;

import cn.doitedu.meta_bus.povo.ServerLogInfo;
import cn.doitedu.meta_bus.povo.TableInfo;
import cn.doitedu.meta_bus.utils.JdbcUtil;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.sql.*;
import java.util.List;

@Controller
public class LogLineCompareController {
    Connection conn;
    public LogLineCompareController() throws SQLException {
        conn = JdbcUtil.getConn();
    }


    @RequestMapping("/api/get")
    @ResponseBody
    public Long getLogServerLogLineCount(String date) throws SQLException {
        // 存入数据库
        PreparedStatement stmt = conn.prepareStatement("select  sum(line_cnt) from logline_monit where log_date=? ");
        stmt.setString(1,date);

        ResultSet resultSet = stmt.executeQuery();
        long cnt = -1;
        while(resultSet.next()){
            cnt = resultSet.getLong(1);
        }
        resultSet.close();
        stmt.close();

        return cnt;
    }


    @RequestMapping("/api/get/{logType}")
    @ResponseBody
    public Long getLogServerLogLineCount2(String date,@PathVariable String logType) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("select sum(line_cnt) from logline_monit where log_date=? and log_type=?");
        stmt.setString(1,date);
        stmt.setString(2,logType);
        ResultSet resultSet = stmt.executeQuery();

        long cnt = -1 ;
        while(resultSet.next()){
            cnt = resultSet.getLong(1);
        }

        resultSet.close();
        stmt.close();

        return cnt;
    }



    @RequestMapping("/api/commit")
    @ResponseBody
    public Integer commitLogLineInfo(@RequestBody  ServerLogInfo info) throws SQLException {

        // 存入数据库
        PreparedStatement stmt = conn.prepareStatement("insert into logline_monit (log_server_name,log_type,log_date,line_cnt) values(?,?,?,?)");
        stmt.setString(1,info.getLogServerName());
        stmt.setString(2,info.getLogType());
        stmt.setString(3,info.getLogDate());
        stmt.setLong(4,info.getLineCnt());

        boolean res = stmt.execute();
        stmt.close();

        return res?1:0;
    }



    // REST API
    @RequestMapping("/api/tableinfo/commit")
    @ResponseBody
    public Integer commitDwTableInfo(@RequestBody List<TableInfo> tableInfos){

        System.out.println(tableInfos);

        return 0;
    }




}
