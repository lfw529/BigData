package kafka.producer;

import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomProducerCallbackPartitions {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 500; i++) {
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
            // 延迟一会儿会看到数据发往不同分区
            Thread.sleep(2);
        }
        kafkaProducer.close();
    }
}
