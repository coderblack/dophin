package cn.doitedu.log_collect.flume;

import com.google.gson.Gson;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class LogTimeExtract implements Interceptor {
    Gson gson = null;
    String keyName;

    public LogTimeExtract(String keyName){
        this.keyName = keyName;
    }


    /**
     * 初始化工作，本方法只会被调用一次
     */
    @Override
    public void initialize() {
        gson = new Gson();
    }

    /**
     * 拦截器的工作逻辑
     *   一次接收一条数据
     *   处理（从数据中提取时间戳，放到header中： timestamp => 1686283785274
     *   返回数据
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        // 从event中拿到数据体（咱们的一条日志）
        String log = new String(event.getBody());

        try {
            // 解析json
            LogBean bean = gson.fromJson(log, LogBean.class);

            // 提取日志时间，放入event的headers中国
            long timestamp = bean.getTimestamp();
            event.getHeaders().put(keyName, timestamp + "");

            return event;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        for (Event event : list) {
            intercept(event);
        }

        return list;
    }

    /**
     * 程序退出前会调用一次，做一些清理工作
     */
    @Override
    public void close() {
        gson = null;
    }


    /**
     * 提供一个拦截器类的对象构造器
     */
    public static class ExtractorBuilder implements Interceptor.Builder{
        String  keyName = "timestamp";
        @Override
        public Interceptor build() {
            return new LogTimeExtract(keyName);
        }

        // a1.sources.s1.interceptors.i1.keyname = timestamp
        @Override
        public void configure(Context context) {
            keyName = context.getString("keyname");

        }
    }
}
