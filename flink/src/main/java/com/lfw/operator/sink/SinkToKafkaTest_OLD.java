package com.lfw.operator.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.charset.Charset;
import java.util.Properties;

public class SinkToKafkaTest_OLD {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置kafka相关参数
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");

        String topic = "flink-kafka";

        String path = new File("").getCanonicalPath() + "/flink/input/clicks.csv";
        DataStreamSource<String> stream = env.readTextFile(path);

        //创建FlinkKafkaProducer, 并输出
        stream.addSink(new FlinkKafkaProducer<String>(
                topic,  //指定 topic
                new KafkaStringSerializationSchema(topic), //指定写入kafka的序列化Schema
                properties, //指定kafka相关参数
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE  //指定写入 Kafka 为 EXACTLY_ONCE 语义
        ));

        env.execute();
    }
}

class KafkaStringSerializationSchema implements KafkaSerializationSchema<String> {
    private String topic;
    private String charset;

    //构造方法传入要写入的 topic 和字符集，默认使用 UTF-8
    public KafkaStringSerializationSchema(String topic) {
        this(topic, "utf-8");
    }

    public KafkaStringSerializationSchema(String topic, String charset) {
        this.topic = topic;
        this.charset = charset;
    }

    //调用该方法将数据进行序列化
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
        //将数据转换成 bytes 数组
        byte[] bytes = element.getBytes(Charset.forName(charset));

        //返回ProducerRecord
        return new ProducerRecord<>(topic, bytes);
    }
}