package com.lfw.sql;

import org.apache.flink.table.api.*;

public class Demo08ColumnDetailTableApi {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        // 建表（数据源表）
        // {"id":4,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
        tenv.createTable("t_person",
                TableDescriptor
                        .forConnector("kafka")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())        // column是声明物理字段到表结构中来
                                .column("name", DataTypes.STRING())   // column是声明物理字段到表结构中来
                                .column("nick", DataTypes.STRING())   // column是声明物理字段到表结构中来
                                .column("age", DataTypes.INT())       // column是声明物理字段到表结构中来
                                .column("gender", DataTypes.STRING())   // column是声明物理字段到表结构中来
                                .columnByExpression("guid", "id")  // 声明表达式字段
                                /*.columnByExpression("big_age",$("age").plus(10))*/      // 声明表达式字段
                                .columnByExpression("big_age", "age + 10")  // 声明表达式字段
                                // isVirtual是表示：当这个表为 sink 表时，该字段是否出现在 schema 中
                                .columnByMetadata("offs", DataTypes.BIGINT(), "offset", true)  // 声明元数据字段
                                .columnByMetadata("ts", DataTypes.TIMESTAMP_LTZ(3), "timestamp", true)  // 声明元数据字段
                                /*.primaryKey("id","name")*/
                                .build())
                        .format("json")
                        .option("topic", "flinksql-08")
                        .option("properties.bootstrap.servers", "hadoop102:9092")
                        .option("properties.group.id", "g1")
                        .option("scan.startup.mode", "earliest-offset")
                        .option("json.fail-on-missing-field", "false")
                        .option("json.ignore-parse-errors", "true")
                        .build()
        );

        tenv.executeSql("select * from t_person").print();
    }
}
