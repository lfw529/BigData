package com.lfw.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;

public class Demo01TableSql {
    public static void main(String[] args) throws DatabaseAlreadyExistException {
        EnvironmentSettings envSettings = EnvironmentSettings.inStreamingMode();  //流计算模式
        TableEnvironment tableEnv = TableEnvironment.create(envSettings);

        //把 kafka 中的一个 topic：flinksql-01 数据映射成一张 flinksql 表
        // json: {"id":1, "name":"zs", "age":28, "gender":"male"}
        // create table_01 (id int, name string, age int, gender string)
        tableEnv.executeSql(
                "create table t_kafka                                  "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql-01',                           "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

        // 用 tableSql 查询计算结果
        tableEnv.executeSql("select gender, avg(age) as avg_age from t_kafka group by gender").print();
    }
}
