package kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerMyPartitioner {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 添加自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "kafka.producer.MyPartitioner");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "lfw " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        System.out.println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition()
                        );
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }

        //再写一个生产者会被阻塞, 因为只创建了一个分区
//        for (int i = 0; i < 5; i++) {
//            kafkaProducer.send(new ProducerRecord<>("first", "wwq " + i), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata metadata, Exception e) {
//                    if (e == null) {
//                        System.out.println("主题：" + metadata.topic() + "->" + "分区：" + metadata.partition()
//                        );
//                    } else {
//                        e.printStackTrace();
//                    }
//                }
//            });
//        }
        kafkaProducer.close();
    }
}
