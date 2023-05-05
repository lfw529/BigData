package com.lfw.operator.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class SourceKafkaTest_NEW {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka中读取数据得到数据流
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("flink_kafka")
                .setGroupId("consumer-group")
                .setBootstrapServers("hadoop102:9092")
                //OffsetInitializer.committedOffsets(OffsetResetStrategy.LATEST) 消费起始位移选择之前所提交的偏移量(如果没有，则重置为LATEST)
                //OffsetsInitializer.earliest()  消费起始位移直接选择为 “最早”
                //OffsetsInitializer.latest()    消费起始位移直接选择为 “最新”
                //OffsetsInitializer.offsets(Map<TopicPartition, Long> offsets)  消费起始位移选择为：方法所传入的每个分区和对应的起始偏移量
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())

                //开启kafka底层消费者的自动位移提交机制
                //它会把最新的消费位移提交到kafka的consumer_offsets中
                //就算把自动位移提交机制开启，KafkaSource依然不依赖自动位移提交机制
                //（宕机重启时，优先从flink自己的状态去获取偏移量<更可靠>）
                .setProperty("auto.offset.commit", "true")

                //把本source算子设置成 BOUNDED 属性 (有界流)，将来本 source 读取数据的时候，读到指定的位置，就停止读取并退出
                //常用于补数或者重跑某一段历史数据
                //.setBounded(OffsetsInitializer.committedOffsets())

                //把本source算子设置成 UNBOUNDED 属性 (无界流)，但是并不会一直读数据，而是打到指定位置就停止读取，但程序不退出
                //主要应用场景：需要从 kafka 中读取某一段固定长度的数据，然后拿着这段数据去跟另外一个真正的无界流联合处理
                //.setUnbounded(OffsetsInitializer.latest());
                .build();

        //env.addSource();   //接收的是 SourceFunction 接口的实现类
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk-source");//接收的是 Source 接口的实现类
        streamSource.print();

        env.execute();
    }
}
