package com.lfw.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 作为 scan source，底层产生 Bounded Stream
 * mysql 建表语句：在 flinksql 库下
 * CREATE TABLE `stu`
 * (
 * `id`              int NOT NULL AUTO_INCREMENT COMMENT 'id',
 * `name`            varchar(255) COMMENT 'name',
 * `age`             int COMMENT '年龄',
 * `gender`          varchar(255) COMMENT '性别',
 * PRIMARY KEY (`id`)
 * ) ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='stu表';
 */
public class Demo17JdbcConnectorSourceScan {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);

        //建表来映射 mysql 中的 flinksql.stu
        tenv.executeSql(
                "create table flink_stu (\n                        " +
                        "   id int primary key,\n                           " +
                        "   name string,\n                                  " +
                        "   age int,\n                                      " +
                        "   gender string\n                                 " +
                        ") with (\n                                         " +
                        "  'connector' = 'jdbc',\n                          " +
                        "  'url' = 'jdbc:mysql://hadoop102:3306/flinksql',\n" +
                        "  'table-name' = 'stu',\n                          " +
                        "  'username' = 'root',\n                           " +
                        "  'password' = '1234' \n                           " +
                        ")"
        );

        DataStreamSource<String> socket = env.socketTextStream("hadoop102", 9999);

        tenv.executeSql("select * from flink_stu").print();

        socket.print();

        env.execute();
    }
}
