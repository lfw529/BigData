package com.lfw.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * >>>>> 练习题需求 >>>>>>>
 * 基本：kafka 中有如下数据：
 * {"id":1,"name":"zs","nick":"tiedan","age":18,"gender":"male"}
 * <p>
 * 高级：kafka 中有如下数据：
 * {"id":1,"name":{"formal":"zs","nick":"tiedan"},"age":18,"gender":"male"}
 * <p>
 * 现在需要用 flinkSql 来对上述数据进行查询统计：
 * 截止到当前,每个昵称,都有多少个用户
 * 截止到当前,每个性别,年龄最大值
 */
public class Demo06Exercise {
    public static void main(String[] args) {
        TableEnvironment tenv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        //建表
        tenv.executeSql(
                "create table t_person                                 "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   nick string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql-06',                           "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

        // 建表（目标表）
        // kafka 连接器，不能接受 UPDATE 修正模式的数据，只能接受 INSERT 模式的数据
        // 而我们的查询语句产生的结果，存在 UPDATE 模式，就需要另一种连接器表 (upsert-kafka) 来接收
        tenv.executeSql(
                "create table t_nick_cnt                               "
                        + " (                                                   "
                        + "   nick string primary key not enforced,             "
                        + "   user_cnt bigint                                   "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'upsert-kafka',                      "
                        + "  'topic' = 'flinksql-nick-06',                      "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'key.format' = 'json' ,                            "
                        + "  'value.format' = 'json'                            "
                        + " )                                                   "
        );

        //查询并打印
        tenv.executeSql(
                "insert into t_nick_cnt " +
                        "select nick, count(distinct id) as user_cnt from t_person group by nick");
    }
}
