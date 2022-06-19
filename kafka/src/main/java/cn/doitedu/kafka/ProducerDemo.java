package cn.doitedu.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.beans.PropertyEditor;
import java.util.Properties;
import java.util.Random;

public class ProducerDemo {

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();

        //设置kafka集群的地址
        props.put("bootstrap.servers", "doit01:9092,doit02:9092,doit03:9092");

        //数据key-value各自的序列化器
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer",StringSerializer.class.getName());



        //ack应答模式，取值有0，1，-1（all）  ， all是最慢但最安全的
        // all 是需要服务端目标分区的所有副本都同步到本次发送的数据，才反馈成功
        // 1   是需要服务端目标分区的leader副本成功存储好这次发送的数据，才反馈成功
        // 0   是不需要服务端做任何反馈
        props.put("acks", "1");

        //失败重试次数（有可能会造成数据的乱序）
        props.put("retries", 3);

        //数据发送的批次大小
        props.put("batch.size", 10);

        //一次发送请求的最大字节数
        props.put("max.request.size",10000);

        //消息在缓冲区保留的时长，超过设置的值就会被提交到服务端
        props.put("linger.ms", 10000);

        //整个Producer用到总内存的大小，如果缓冲区满了会提交数据到服务端
        //buffer.memory要大于batch.size，否则会报申请内存不足的错误
        props.put("buffer.memory", 10240);


        // 利用参数配置，构造一个kafka的生产者客户端
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();
        for (int i = 0; i <= 100; i++) {
            Thread.sleep(100);

            // 构造自己的数据
            String key = i + "";
            String msg = key + " hello doitEdu,you are great ";

            // 把自己的数据封装成producer所要求的格式
            ProducerRecord<String, String> record = new ProducerRecord<>("doitedu-01", key, msg);

            // 把数据发到kafka中去
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }
}
