import cn.doitedu.log_collect.flume.LogBean;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.HashMap;
import java.util.Map;

public class GsonTest {

    public static void main(String[] args) {


        String line = "{\"account\":\"轩辕放\",\"appid\":\"cn.doitedu.study.Yiee\",\"appversion\":\"7.6\",\"carrier\":\"爱奇艺移动\",\"deviceid\":\"XDUSUCKCPOPH\",\"devicetype\":\"REDMI-6\",\"eventid\":\"pageView\",\"ip\":\"32.29.73.98\",\"latitude\":37.597446232029824,\"longitude\":118.56529105133706,\"nettype\":\"4G\",\"osname\":\"android\",\"osversion\":\"7.5\",\"properties\":{\"refUrl\":\"/shares/sha0642.html\",\"pageId\":\"tea0200\",\"url\":\"/teachers/tea0200.html\"},\"releasechannel\":\"小米应用商店\",\"resolution\":\"1024*768\",\"sessionid\":\"rtgmimyw\",\"timestamp\":1644586253881}";
        Gson gson = new Gson();
        LogBean bean  = gson.fromJson(line, LogBean.class);
        System.out.println(bean.getTimestamp());



    }
}
