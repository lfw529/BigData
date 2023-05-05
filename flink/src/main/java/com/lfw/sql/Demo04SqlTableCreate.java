package com.lfw.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo04SqlTableCreate {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);

        // TODO: 1.通过构建一个 TableDescriptor 来创建一个“有名”表 (sql表)
        tenv.createTable("table_1",  //表名
                TableDescriptor.forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.STRING())
                                .column("age", DataTypes.INT())
                                .column("gender", DataTypes.STRING())
                                .build())
                        .format("csv")
                        .option("path", "flink/input/a.txt")
                        .option("csv.ignore-parse-errors", "true")  //如果 csv 文件有脏数据则会被忽略，设置为 false，则会抛出异常。
                        .build());

        tenv.executeSql("select * from table_1").print();
//        System.exit(1); // 退出后，后面的语句无法输出
        tenv.executeSql("select gender, max(age) as max_age from table_1 group by gender").print();


        // TODO: 2.从一个dataStream上创建“有名”的视图
        DataStreamSource<String> stream1 = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Demo03ApiTableCreate.Person> javaBeanStream = stream1.map(s -> {
            String[] split = s.split(",");
            return new Demo03ApiTableCreate.Person(Integer.parseInt(split[0]), split[1], Integer.parseInt(split[2]), split[3]);
        });
        tenv.createTemporaryView("t_person", javaBeanStream);
        tenv.executeSql("select gender, max(age) as max_age from t_person group by gender")/*.print()*/;

        // TODO: 3.从一个已存在table对象，得到一个“有名”的视图
        Table table_x = tenv.from("table_1");
        tenv.createTemporaryView("table_3", table_x);
        tenv.executeSql("select * from table_3").print();

        //TODO: 4.通过 sql DDL 语句定义
        tenv.executeSql(
                "create temporary table table_4                        "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql-04',                           "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );
        tenv.executeSql("select * from table_4").print();
    }
}
