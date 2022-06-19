package cn.doitedu.meta_bus.controller;

import cn.doitedu.meta_bus.povo.LineData;
import cn.doitedu.meta_bus.utils.JdbcUtil;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.sql.*;
import java.util.ArrayList;

@Controller
public class LoginController {

    Connection conn;
    public LoginController() throws SQLException {
        conn = JdbcUtil.getConn();
    }



    @RequestMapping("/login")
    public String login(String account, String password , Model model){
        System.out.println(account);
        System.out.println(password);

        model.addAttribute("name",account);

        return "home";
    }


    @RequestMapping("/showline")
    public String showLogline(Model model) throws SQLException {

        PreparedStatement stmt = conn.prepareStatement("select * from logline_monit");
        ResultSet resultSet = stmt.executeQuery();
        ArrayList<LineData> datas = new ArrayList<>();
        while(resultSet.next()){
            String host = resultSet.getString("log_server_name");
            String date = resultSet.getString("log_date");
            int cnt = resultSet.getInt("line_cnt");
            String logType = resultSet.getString("log_type");

            LineData lineData = new LineData(date, host, logType, cnt);
            datas.add(lineData);

        }

        model.addAttribute("datas",datas);
        return "loglines";
    }



}
