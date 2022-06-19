package cn.doitedu.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 事务机制练习
 * 场景： 1. 程序要从kafka的一个topic中读取数据做处理
 * 2.要自己记录处理到的位置（消费偏移量）
 * 3.让数据处理的结果和偏移量的记录，放在一个事务中，要么都成功，要么都回滚
 */
public class TransactionDemo {

    public static void main(String[] args) throws SQLException {
        // 构造一个生产者用于输出数据处理结果（输出大写的字母结果）
        KafkaProducer<String, String> producer = getTransactionalProducer();
        producer.initTransactions();

        // 构造一个读数据源的消费者（读一些小写的a、b、c、d字母）
        KafkaConsumer<String, String> consumer = Exercise2.getConsumer("trans_demo");

        // 拿到mysql的jdbc连接
        Connection conn = Exercise2.getConn();  // doit01,abc
        PreparedStatement stmt = conn.prepareStatement("insert into offset_meta(topic_partition,offset) values(?,?) ON DUPLICATE KEY UPDATE offset=?");

        // 定于数据源主题
        consumer.subscribe(Collections.singletonList("trans-tpc"));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                // 数据处理： 小写变大写
                String result = value.toUpperCase();

                // 把处理结果写入kafka，并提交偏移量到mysql
                String topic = record.topic();
                int partition = record.partition();
                long offset = record.offset();

                stmt.setString(1,topic+"-"+partition);
                stmt.setLong(2,offset);
                stmt.setLong(3,offset);

                // 开启事务
                producer.beginTransaction();
                try {

                    // 写出数据处理结果到目标的kafka主题
                    ProducerRecord<String, String> rec = new ProducerRecord<>("trans-res", result);
                    producer.send(rec);

                    // TODO  可以在这里安排 抛出异常(比如产生随机数%7==0,就抛出异常)，来检验事务管理是否真正生效

                    // 执行偏移量提交sql
                    stmt.execute();

                    // 提交事务
                   producer.commitTransaction();

                }catch (Exception e){

                    // 放弃事务（回滚事务）
                    producer.abortTransaction();
                }
            }
        }
    }

    public static KafkaProducer<String,String> getTransactionalProducer(){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"doit01:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks
        props.setProperty(ProducerConfig.ACKS_CONFIG,"-1");
        // 生产者的重试次数
        props.setProperty(ProducerConfig.RETRIES_CONFIG,"3");
        // 飞行中的请求缓存最大数量
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"3");
        // 开启幂等性
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        // 设置事务id
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"trans_001");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        return producer;
    }


}
