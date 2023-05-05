package com.lfw.operator.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class SourceKafkaTest_OLD {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置kafka相关参数
        Properties properties = new Properties();
        //设置kafka的地址和端口号
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //设置读取偏移量策略：最早开始读取
        properties.setProperty("auto.offset.reset", "earliest");
        //设置消费者组id
        properties.setProperty("group.id", "consumer-group");
        //没有开启checkpoint，让flink提交偏移量的消费者定期自动提交偏移量
        properties.setProperty("enable.auto.commit", "true");
        //创建FlinkKafkaConsumer并传入相关参数
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "flink_kafka",    //要读取数据的 topic
                new SimpleStringSchema(),  //读取文件的反序列化 Schema
                properties   //传入 kafka 的参数
        );

        //使用 addSource 添加 kafkaConsumer
        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        lines.print();
        env.execute();
    }
}
