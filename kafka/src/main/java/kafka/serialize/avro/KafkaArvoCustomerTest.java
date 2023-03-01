package kafka.serialize.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaArvoCustomerTest {
    public static void main(String[] args) {
        //1.创建消费者的配置对象
        Properties properties = new Properties();
        //2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //配置反序列化 必须
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        //配置注册表 confluent 需要
        properties.put("schema.registry.url", "http://192.168.6.102:8081");

        properties.put("group.id", "customer");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(properties);

        //订阅主题
//        consumer.subscribe(Collections.singletonList("customerContacts"));  //从分区的最新偏移量开始消费
        //从分区的开头开始
        TopicPartition tp = new TopicPartition("customerContacts", 0);
        List<TopicPartition> list = new ArrayList<>();
        list.add(tp);

        //需要指派主题分区列表
        consumer.assign(list);
        //从指定主题的特定分区开始
        consumer.seekToBeginning(Collections.singleton(tp));
        //轮询
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    try {
                        System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("测试消费者！");
        }
    }
}
