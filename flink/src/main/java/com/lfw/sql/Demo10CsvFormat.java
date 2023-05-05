package com.lfw.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo10CsvFormat {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inBatchMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        /**
         * 读取 csv 格式的文件形成表
         * 测试数据：
         *    |1|,|zs|,|18|
         *    # 哈哈哈哈
         *    |2|,|ls|,|20|
         *    |3|,|ww|,|AAA|
         */
        tenv.executeSql(
                 "create table t_csv (                          "
                        + "   id int,                                    "
                        + "   name string,                               "
                        + "   age string                                 "
                        + ") with (                                      "
                        + " 'connector' = 'filesystem',                  "
                        + " 'path' = 'flink/input/csv/',                 "
                        + " 'format' = 'csv',                            "
                        + " 'csv.disable-quote-character' = 'false',     "
                        + " 'csv.quote-character' = '|',                 "
                        + " 'csv.ignore-parse-errors' = 'true',          "
                        + " 'csv.null-literal' = 'AAA',                  "
                        + " 'csv.allow-comments' = 'true'                "
                        + ")                                             "
        );

        tenv.executeSql("desc t_csv").print();
//        +------+--------+------+-----+--------+-----------+
//        | name |   type | null | key | extras | watermark |
//        +------+--------+------+-----+--------+-----------+
//        |   id |    INT | true |     |        |           |
//        | name | STRING | true |     |        |           |
//        |  age | STRING | true |     |        |           |
//        +------+--------+------+-----+--------+-----------+
        tenv.executeSql("select * from t_csv").print();
//        +-------------+--------------------------------+--------------------------------+
//        |          id |                           name |                            age |
//        +-------------+--------------------------------+--------------------------------+
//        |           1 |                             zs |                             18 |
//        |           3 |                             ww |                         (NULL) |
//        |           2 |                             ls |                             20 |
//        +-------------+--------------------------------+--------------------------------+
    }
}
