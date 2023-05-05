package com.lfw.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Demo05Catalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //环境创建之初，底层会根据自动初始化一个元数据空间实现对象 (default_catalog => GenericInMemoryCatalog)
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //创建了一个 hive 元数据空间的实现对象 [需要提前打开hive的metastore: hive --service metastore]
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "flink/conf");
        //将 hive 元数据空间对象注册到环境中
        tenv.registerCatalog("mycatalog", hiveCatalog);

        tenv.executeSql(
                "create temporary table `mycatalog`.`default`.`t1_person`    "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql-05',                           "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

        tenv.executeSql(
                "create temporary table `t2_person`    "
                        + " (                                                   "
                        + "   id int,                                           "
                        + "   name string,                                      "
                        + "   age int,                                          "
                        + "   gender string                                     "
                        + " )                                                   "
                        + " WITH (                                              "
                        + "  'connector' = 'kafka',                             "
                        + "  'topic' = 'flinksql-05',                           "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092', "
                        + "  'properties.group.id' = 'g1',                      "
                        + "  'scan.startup.mode' = 'earliest-offset',           "
                        + "  'format' = 'json',                                 "
                        + "  'json.fail-on-missing-field' = 'false',            "
                        + "  'json.ignore-parse-errors' = 'true'                "
                        + " )                                                   "
        );

        tenv.executeSql("create view if not exists `mycatalog`.`default`.`t_person_view` as select id, name, age from `mycatalog`.`default`.`t1_person`");

        // 列出当前会话中所有的catalog
        tenv.listCatalogs();

        // 列出 default_catalog 中的库和表
        tenv.executeSql("show catalogs").print();
        tenv.executeSql("use catalog default_catalog");
        tenv.executeSql("show databases").print();
        tenv.executeSql("use default_database");
        tenv.executeSql("show tables").print();

        System.out.println("----------------------");

        // 列出 mycatalog 中的库和表
        tenv.executeSql("use catalog mycatalog");
        tenv.executeSql("show databases").print();
        tenv.executeSql("use `default`");
        tenv.executeSql("show tables").print();

        System.out.println("----------------------");

        // 列出临时表
        tenv.listTemporaryTables();
    }
}
