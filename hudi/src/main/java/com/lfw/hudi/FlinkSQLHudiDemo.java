package com.lfw.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 基于Flink SQL Connector实现：实时消费Topic中数据，转换处理后，实时存储Hudi表中
 */
public class FlinkSQLHudiDemo {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "lfw");

        //1.获取表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //2.创建输入表，TODO: 从 kafka 消费数据
        tableEnv.executeSql(
                "CREATE TABLE order_kafka_source (\n" +
                        "   id BIGINT,\n" +
                        "   order_id BIGINT,\n" +
                        "   status STRING,\n" +
                        "   refund_money DECIMAL(20, 4),\n" +
                        "   order_products STRING,\n" +
                        "   refund_order_id BIGINT,\n" +
                        "   refund_price DECIMAL(20, 4),\n" +
                        "   refund_org_coupon_price DECIMAL(20, 4),\n" +
                        "   refund_platform_coupon_price DECIMAL(20, 4),\n" +
                        "   refund_express_price DECIMAL(20, 4),\n" +
                        "   refund_type STRING,\n" +
                        "   before_order_status STRING,\n" +
                        "   created_at STRING,\n" +
                        "   updated_at STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'ods.svc-trade.order_refund',\n" +
                        "  'properties.bootstrap.servers' = '10.18.180.21:9092,10.18.180.22:9092,10.18.180.23:9092',\n" +
                        "  'properties.group.id' = 'dev_test',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.fail-on-missing-field' = 'false',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        //3.数据转换：提取订单时间中订单日期，作为Hudi表分区字段值
        Table etlTable = tableEnv.from("order_kafka_source")
                .addColumns($("created_at").substring(0, 10).as("partition_day"))
                .addColumns($("created_at").substring(0, 19).as("ts"));
        tableEnv.createTemporaryView("view_order_refund", etlTable);

        //4.定义输出表，TODO：数据保存到Hudi表中
        tableEnv.executeSql(
                "CREATE TABLE order_hudi_sink (\n" +
                        "   id BIGINT PRIMARY KEY NOT ENFORCED,\n" +
                        "   order_id BIGINT,\n" +
                        "   status STRING,\n" +
                        "   refund_money DECIMAL(20, 4),\n" +
                        "   order_products STRING,\n" +
                        "   refund_order_id BIGINT,\n" +
                        "   refund_price DECIMAL(20, 4),\n" +
                        "   refund_org_coupon_price DECIMAL(20, 4),\n" +
                        "   refund_platform_coupon_price DECIMAL(20, 4),\n" +
                        "   refund_express_price DECIMAL(20, 4),\n" +
                        "   refund_type STRING,\n" +
                        "   before_order_status STRING,\n" +
                        "   created_at STRING,\n" +
                        "   updated_at STRING,\n" +
                        "   ts STRING,\n" +
                        "   partition_day STRING\n" +
                        ")\n" +
                        "PARTITIONED BY (partition_day) \n" +
                        "WITH (\n" +
                        "  'connector' = 'hudi',\n" +
                        "  'path' = 'file:///D:/flink_hudi_order',\n" +
                        "  'table.type' = 'MERGE_ON_READ',\n" +
                        "  'write.operation' = 'upsert',\n" +
                        "  'hoodie.datasource.write.recordkey.field' = 'id'," +
                        "  'write.precombine.field' = 'ts'," +
                        "  'write.tasks'= '1'" +
                        ")"
        );

        //5.通过子查询方式，将数据写入输出表
        tableEnv.executeSql(
                "INSERT INTO order_hudi_sink\n" +
                        "SELECT\n" +
                        " id, order_id, status, refund_money, order_products, refund_order_id, refund_price, refund_org_coupon_price," +
                        " refund_platform_coupon_price, refund_express_price, refund_type, before_order_status, created_at, updated_at, ts, partition_day\n" +
                        "FROM view_order_refund"
        );
    }
}
