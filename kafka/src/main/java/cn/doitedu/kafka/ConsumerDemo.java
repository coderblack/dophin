package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {

        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "doit01:9092,doit02:9092,doit03:9092");

        // 指定consumer group
        props.put("group.id", "g1");

        // 是否自动提交消费偏移量offset
        props.put("enable.auto.commit", "true");   // 如果需要精细化地手动控制消费位移提交，才需要关闭自动提交参数

        // 自动提交offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");

        // key的反序列化类
        props.put("key.deserializer", StringDeserializer.class.getName());
        // value的反序列化类
        props.put("value.deserializer", StringDeserializer.class.getName());

        // 如果没有消费偏移量记录，则自动重设为起始offset：latest, earliest, none
        props.put("auto.offset.reset","earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 订阅主题
        consumer.subscribe(Collections.singletonList("topic-doit29"));  // 可以用正则模式来实现动态订阅


        // 进入数据拉取循环
        while(true){

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                String topic = record.topic();
                String key = record.key();
                String value = record.value();
                int partition = record.partition();
                long offset = record.offset();
                System.out.println(String.format("topic:%s ,partition:%d ,offset:%d , key:%s ,value:%s",topic,partition,offset,key,value ));
            }
        }



    }
}
