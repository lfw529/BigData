package com.lfw.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo09JsonFormat {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        /* *
         * 1. 简单嵌套 json 建表示例
         * 嵌套对象解析成 Map 类型
         * {"id":12,"name":{"nick":"lfw3","formal":"lfw edu3","height":170}}
         */
        tenv.executeSql(
                "create table t_json1 (                       "
                        + "  id int,                                   "
                        + "  name map<string, string>,                 "
                        + "  bigid as id * 10                          "
                        + ")with(                                      "
                        + " 'connector' = 'filesystem',                "
                        + " 'path' = 'flink/input/json/qiantao1/',           "
                        + " 'format'='json'                            "
                        + ")                                           "
        );
        tenv.executeSql("desc t_json1")/*.print()*/;
//        +-------+---------------------+------+-----+--------------+-----------+
//        |  name |                type | null | key |       extras | watermark |
//        +-------+---------------------+------+-----+--------------+-----------+
//        |    id |                 INT | true |     |              |           |
//        |  name | MAP<STRING, STRING> | true |     |              |           |
//        | bigid |                 INT | true |     | AS `id` * 10 |           |
//        +-------+---------------------+------+-----+--------------+-----------+
        tenv.executeSql("select * from t_json1")/*.print()*/;
//        +----+-------------+--------------------------------+-------------+
//        | op |          id |                           name |       bigid |
//        +----+-------------+--------------------------------+-------------+
//        | +I |          12 | {nick=lfw3, formal=lfw edu3... |         120 |
//        +----+-------------+--------------------------------+-------------+
        // 查询每个人的id和nick
        tenv.executeSql("select id, name['nick'] as nick from t_json1")/*.print()*/;
//        +----+-------------+--------------------------------+
//        | op |          id |                           nick |
//        +----+-------------+--------------------------------+
//        | +I |          12 |                           lfw3 |
//        +----+-------------+--------------------------------+

        /* *
         * 2. 简单嵌套 json 建表示例
         * 嵌套对象，解析成 Row 类型
         * {"id":12,"name":{"nick":"lfw3","formal":"lfw edu3","height":170}}
         *   id int,
         *   name row<nick string, formal string, height int>
         */
        tenv.createTable("t_json2",
                TableDescriptor
                        .forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("id", DataTypes.INT())
                                .column("name", DataTypes.ROW(
                                        DataTypes.FIELD("nick", DataTypes.STRING()),
                                        DataTypes.FIELD("formal", DataTypes.STRING()),
                                        DataTypes.FIELD("height", DataTypes.INT())
                                ))
                                .build())
                        .format("json")
                        .option("path", "flink/input/json/qiantao2/")
                        .build());

        tenv.executeSql("desc t_json2")/*.print()*/;
//        +------+---------------------------------------------------+------+-----+--------+-----------+
//        | name |                                              type | null | key | extras | watermark |
//        +------+---------------------------------------------------+------+-----+--------+-----------+
//        |   id |                                               INT | true |     |        |           |
//        | name | ROW<`nick` STRING, `formal` STRING, `height` INT> | true |     |        |           |
//        +------+---------------------------------------------------+------+-----+--------+-----------+
        tenv.executeSql("select * from t_json2")/*.print()*/;
//        +----+-------------+--------------------------------+
//        | op |          id |                           name |
//        +----+-------------+--------------------------------+
//        | +I |          12 |        +I[lfw3, lfw edu3, 170] |
//        +----+-------------+--------------------------------+
        // 查询每个人的 id 和 formal 名和 height
        tenv.executeSql("select id, name.formal, name.height from t_json2")/*.print()*/;
//        +----+-------------+--------------------------------+-------------+
//        | op |          id |                         formal |      height |
//        +----+-------------+--------------------------------+-------------+
//        | +I |          12 |                       lfw edu3 |         170 |
//        +----+-------------+--------------------------------+-------------+

        /* *
         * 3. 复杂嵌套 json，建表示例
         * {"id":1, "friends":[{"name":"a","info":{"addr":"bj","gender":"male"}},{"name":"b","info":{"addr":"sh","gender":"female"}}]}
         */
        tenv.executeSql(
                "create table t_json3 (                                         "
                        + "   id int,                                                    "
                        + "   friends array<row<name string, info map<string, string>>>  "
                        + ") with (                                                      "
                        + "   'connector' = 'filesystem',                                "
                        + "   'path' = 'flink/input/json/qiantao3/',                     "
                        + "   'format'='json'                                            "
                        + ")                                                             "
        );

        tenv.executeSql("desc t_json3").print();
//        +---------+-------------------------------------------------------+------+-----+--------+-----------+
//        |    name |                                                  type | null | key | extras | watermark |
//        +---------+-------------------------------------------------------+------+-----+--------+-----------+
//        |      id |                                                   INT | true |     |        |           |
//        | friends | ARRAY<ROW<`name` STRING, `info` MAP<STRING, STRING>>> | true |     |        |           |
//        +---------+-------------------------------------------------------+------+-----+--------+-----------+
        tenv.executeSql("select * from t_json3").print();
//        +----+-------------+--------------------------------+
//        | op |          id |                        friends |
//        +----+-------------+--------------------------------+
//        | +I |           1 | [+I[a, {gender=male, addr=b... |
//        +----+-------------+--------------------------------+
        tenv.executeSql("select id," +
                "friends[1].name as name1, friends[1].info['addr'] as addr1, friends[1].info['gender'] as gender1, " +
                "friends[2].name as name2, friends[2].info['addr'] as addr2, friends[2].info['gender'] as gender2  " +
                "from  t_json3").print();
//        +----+-------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
//        | op |          id |                          name1 |                          addr1 |                        gender1 |                          name2 |                          addr2 |                        gender2 |
//        +----+-------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
//        | +I |           1 |                              a |                             bj |                           male |                              b |                             sh |                         female |
//        +----+-------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
    }
}
