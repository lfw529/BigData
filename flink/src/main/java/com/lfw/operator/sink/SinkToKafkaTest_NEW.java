package com.lfw.operator.sink;

import com.alibaba.fastjson.JSON;
import com.lfw.operator.source.MySourceFunction;
import com.lfw.pojo.EventLog;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.io.File;

public class SinkToKafkaTest_NEW {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // 开启checkpoint
        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        String path = new File("").getCanonicalPath() + "/flink/input/clicks.csv";

        DataStreamSource<EventLog> stream = env.addSource(new MySourceFunction());   //继承了 SourceFunction, 所以单并行度

        // 把数据写入kafka
        // 1. 构造一个kafka的sink算子
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("flink-kafka")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setTransactionalIdPrefix("lfw-")
                .build();

        //2. 把数据流输出到构造好的sink算子   //多并行度
        stream.map(JSON::toJSONString).disableChaining()
                .sinkTo(kafkaSink);

        env.execute();
    }
}
