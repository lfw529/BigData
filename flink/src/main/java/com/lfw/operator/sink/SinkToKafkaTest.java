package com.lfw.operator.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.File;
import java.util.Properties;

public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");

        String path = new File("").getCanonicalPath() + "/flink/input/clicks.csv";
        DataStreamSource<String> stream = env.readTextFile(path);

        stream.addSink(new FlinkKafkaProducer<String>(
                "flink_kafka",
                new SimpleStringSchema(),
                properties
        ));

        env.execute();
    }
}
