package cn.doitedu.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;

/**
 * 练习题：
 * 1. 从本地文件 data/access_log/app.access.log.2022-02-26 中读取每一行，写入kafka
 * <p>
 * 2. 从kafka中读取练习1所写入的数据，提取出其中的deviceId,eventId,deviceType,timestamp信息，写入mysql
 */
public class Exercise1 {

    public static void main(String[] args) throws Exception {
        KafkaProducer<String, String> producer = getProducer();

        BufferedReader br = new BufferedReader(new FileReader("data/access_log/app.access.log.2022-02-26"));
        String line;
        while ((line = br.readLine()) != null) {
            if (StringUtils.isNotBlank(line)) {
                System.out.println(line);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("access_log", line);
                producer.send(record);
                producer.flush();
            }
        }

        producer.close();
    }

    public static KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();

        //设置kafka集群的地址
        props.put("bootstrap.servers", "doit01:9092,doit02:9092,doit03:9092");

        //数据key-value各自的序列化器
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
       /* props.put("acks", "1");

        //失败重试次数（有可能会造成数据的乱序）
        props.put("retries", 3);

        //数据发送的批次大小
        props.put("batch.size", 10);

        //一次请求（一条消息或一批消息）的最大字节数
        props.put("max.request.size", 100);

        //消息在缓冲区保留的时长，超过设置的值就会被提交到服务端
        props.put("linger.ms", 10000);

        //整个Producer用到总内存的大小，如果缓冲区满了会提交数据到服务端
        //buffer.memory要大于batch.size，否则会报申请内存不足的错误
        props.put("buffer.memory", 10240);*/

        return new KafkaProducer<>(props);
    }

}
