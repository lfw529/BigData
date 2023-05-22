package com.lfw.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;


public class FlinkCDC_CustomSchema {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Flink-CDC 将读取 binlog 的位置信息以状态方式保存在CK,如果想要做到断点续传，需要从Checkpoint或者Savepoint启动程序
        //开启Checkpoint，每隔5s钟做一次CK
        env.enableCheckpointing(5000L);
        //指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置任务关闭的时候保留最后一次 CK 数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend()).getCheckpointConfig().
                setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/flink/checkpoint"));
        //设置访问 HDFS 的用户名
        System.setProperty("HADOOP_USER_NAME", "lfw");

        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("flink-cdc")
                .tableList("flink-cdc.test01") //可选配置项，如果不指定该参数，则会读取上一个配置下所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .username("root")
                .password("1234")
                .deserializer(new MyDeserializationSchema())  //官方提供的序列化
                .build();

        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //3.打印
        mysqlDS.print();

        //4.启动
        env.execute();
    }

    // 自定义数据解析器
    public static class MyDeserializationSchema implements DebeziumDeserializationSchema<String> {
        /**
         * {
         * "data":"{"id":11,"tm_name":"sasa"}",
         * "db":"",
         * "tableName":"",
         * "op":"c u d",
         * "ts":""
         * }
         */
        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //获取主题信息，提取数据库和表名
            String topic = sourceRecord.topic();
            String[] arr = topic.split("\\.");
            String db = arr[1];
            String tableName = arr[2];

            //获取操作类型 READ DELETE UPDATE CREATE
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);

            //获取值信息并转换为 Struct 类型
            Struct value = (Struct) sourceRecord.value();

            //获取变化后的数据
            Struct after = value.getStruct("after");

            JSONObject jsonObject = new JSONObject();    //json 空对象
            for (Field field : after.schema().fields()) {
                Object o = after.get(field);
                jsonObject.put(field.name(), o);
            }

            //创建结果类型
            JSONObject result = new JSONObject();
            result.put("database", db);
            result.put("tableName", tableName);
            result.put("data", jsonObject);
            result.put("op", operation);

            //输出数据
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return BasicTypeInfo.STRING_TYPE_INFO;
        }
    }
}
