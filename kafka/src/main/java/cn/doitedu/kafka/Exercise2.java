package cn.doitedu.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 2. 从kafka中读取练习1所写入的数据，提取出其中的deviceId,eventId,deviceType,timestamp信息，写入mysql
 */
public class Exercise2 {

    public static void main(String[] args) throws SQLException {

        KafkaConsumer<String, String> consumer = getConsumer("ggg");
        consumer.subscribe(Collections.singletonList("access_log"));

        Connection conn = getConn();
        PreparedStatement statement = conn.prepareStatement("insert into kafka_exercise values (?,?,?,?)");

        boolean flag = true;
        while(flag){
            long start_time = System.currentTimeMillis();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100000));
            for (ConsumerRecord<String, String> record : records) {
                String json = record.value();
                System.out.println(json);
                // 解析json数据，抽取所要的字段
                JSONObject jsonObject = JSON.parseObject(json);
                String deviceId = jsonObject.getString("deviceid");
                String eventId = jsonObject.getString("eventid");
                String deviceType = jsonObject.getString("devicetype");
                long timestamp = jsonObject.getLong("timestamp");

                statement.setString(1,deviceId);
                statement.setString(2,eventId);
                statement.setString(3,deviceType);
                statement.setLong(4,timestamp);

                statement.execute();
            }

            if(System.currentTimeMillis() - start_time > 800000) flag = false;

        }

        conn.close();
        consumer.close();
        System.out.println("我爱你，拜拜");

    }


    public static KafkaConsumer<String,String> getConsumer(String groupId){
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "doit01:9092,doit02:9092,doit03:9092");

        // 指定consumer group
        props.put("group.id", groupId);

        // 是否自动提交消费偏移量offset
        //props.put("enable.auto.commit", "true");   // 如果需要精细化地手动控制消费位移提交，才需要关闭自动提交参数
        props.put("enable.auto.commit", "false");   // 做事务测试时特意修改的

        // 自动提交offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");

        // key的反序列化类
        props.put("key.deserializer", StringDeserializer.class.getName());
        // value的反序列化类
        props.put("value.deserializer", StringDeserializer.class.getName());

        // 如果没有消费偏移量记录，则自动重设为起始offset：latest, earliest, none
        props.put("auto.offset.reset","earliest");

        return new KafkaConsumer<String, String>(props);
    }

    public static Connection getConn() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://doit01:3306/abc","root","ABC123.abc123");
    }
}
