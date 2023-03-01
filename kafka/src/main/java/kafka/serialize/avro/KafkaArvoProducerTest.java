package kafka.serialize.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Properties;

public class KafkaArvoProducerTest {
    public static void main(String[] args) {
        String schemaString = "{\n" +
                "  \"namespace\": \"kafka.serialize.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Customer\",\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"name\": \"id\",\n" +
                "      \"type\": \"int\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"name\",\n" +
                "      \"type\": \"string\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"email\",\n" +
                "      \"type\": [\n" +
                "        \"null\",\n" +
                "        \"string\"\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        //1.创建消费者的配置对象
        Properties properties = new Properties();
        //2.给消费者配置对象添加参数
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        //配置序列化 必须
        properties.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        //配置注册表 [没有会报错：Caused by: io.confluent.common.config.ConfigException: Missing required configuration "schema.registry.url" which has no default value.]
        properties.put("schema.registry.url", "http://192.168.6.102:8081");

        //3.创建kafka生产者对象
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties);

        String topic = "customerTest";

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);
        GenericRecord arvoRecord = new GenericData.Record(schema);

        try {
            //4.发送信息
            for (int i = 0; i < 10; i++) {
                String name = "customers" + i;
                String email = "customers" + i + "@qq.com";

                arvoRecord.put("id", i);
                arvoRecord.put("name", name);
                arvoRecord.put("email", email);
                System.out.println("Generated customer" + arvoRecord);

                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, arvoRecord);
                producer.send(record);
            }
        } catch (SerializationException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
