package com.lfw.hudi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 基于 FlinkSQL Connector 实现：实现消费Topic中数据，转换处理后，实时存储Hudi表中
 */
public class FlinkSQLKafkaDemo {
    public static void main(String[] args) {
        //1-获取表执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 2-创建输入表, TODO: 从Kafka消费数据
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
        // 3-数据转换：提取订单时间中订单日期，作为Hudi表分区字段值
        Table etlTable = tableEnv.from("order_kafka_source")
                .addColumns($("created_at").substring(0, 10).as("partition_day"))
                .addColumns($("created_at").substring(0, 19).as("ts"));
        tableEnv.createTemporaryView("view_order_refund", etlTable);
        // 4-查询数据
        tableEnv.executeSql("SELECT * FROM view_order_refund").print();
    }
}
