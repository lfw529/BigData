package com.lfw.sql;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class Demo03ApiTableCreate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 假设 table_1 已经被创建过
        /* tenv.executeSql("create table table_1(id int,name string) " +
                "with ('connector'='kafka'," +
                "......"); */

        // TODO: 1.从一个已存在的表名，来创建table对象
        // Table table_1 = tenv.from("table_1");

        // TODO: 2.从一个TableDescriptor来创建table对象
        Table table_2 = tenv.from(TableDescriptor
                .forConnector("kafka")  //指定连接器
                .schema(Schema.newBuilder()  //指定表结构
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build())
                .format("json")  //指定数据源的数据格式
                .option("topic", "flinksql-03")   //连接器及format格式的相关参数
                .option("properties.bootstrap.servers", "hadoop102:9092")
                .option("properties.group.id", "g2")
                .option("scan.startup.mode", "earliest-offset")
                .option("json.fail-on-missing-field", "false")
                .option("json.ignore-parse-errors", "true")
                .build());

        // table_2.execute().print();

        // TODO: 3.从数据流来创建table对象
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("flinksql-03")
                .setGroupId("g2")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        DataStreamSource<String> kfkStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk");
        /* kfkStream.print();*/

        //3.1 不指定schema，将流创建成Table对象，表的schema是默认的，往往不符合我们的要求 [自动推断]
        Table table_3_1 = tenv.fromDataStream(kfkStream);
        /* table_3_1.execute().print();*/

        //3.2 为了获得更理想的表结构，可以先把数据流中的数据转成javabean类型
        SingleOutputStreamOperator<Person> javaBeanStream = kfkStream.map(json -> JSON.parseObject(json, Person.class));
        Table table_3_2 = tenv.fromDataStream(javaBeanStream);
        /*table_3_2.execute().print();*/

        // 3.3 手动指定schema定义，来将一个javabean流，转成Table对象
        Table table_3_3 = tenv.fromDataStream(javaBeanStream,
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("name", DataTypes.STRING())
                        .column("age", DataTypes.INT())
                        .column("gender", DataTypes.STRING())
                        .build());
        /*table_3_3.printSchema();
        table_3_3.execute().print();*/

        //TODO: 4.用测试数据来得到一个表对象
        //4.1 但字段数据建测试表
        Table table_4_1 = tenv.fromValues(1, 2, 3, 4, 5);
        table_4_1.printSchema();
        table_4_1.execute().print();

        //4.2 多字段数据建测试表
        Table table_4_2 = tenv.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.DOUBLE())
                ),
                Row.of(1, "zs", 18.2),
                Row.of(2, "bb", 28.2),
                Row.of(3, "cc", 16.2),
                Row.of(4, "dd", 38.2)
        );
        table_4_2.printSchema();
        table_4_2.execute().print();

        env.execute();
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        public int id;
        public String name;
        public int age;
        public String gender;
    }
}
