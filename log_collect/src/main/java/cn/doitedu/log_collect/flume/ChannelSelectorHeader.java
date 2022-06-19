package cn.doitedu.log_collect.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;
import java.util.Random;

public class ChannelSelectorHeader implements Interceptor {
    Random rd;
    @Override
    public void initialize() {
        rd = new Random();
    }

    @Override
    public Event intercept(Event event) {
        event.getHeaders().put("state",rd.nextInt(2)+"");
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() { }


    public static class ChannelSelectorHeaderBuilder implements Interceptor.Builder{
        @Override
        public Interceptor build() {
            return new ChannelSelectorHeader();
        }

        @Override
        public void configure(Context context) {

        }
    }



}
