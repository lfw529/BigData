package com.lfw.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Demo20StreamFromToTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///e:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        //1,zs,male,18
        //2,ls,female,22
        //3,ww,male,17
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Person> s2 = s1.map(s -> {
            String[] split = s.split(",");
            return new Person(Integer.parseInt(split[0]), split[1], split[2], Integer.parseInt(split[3]));
        });

        //把流变成表
        tenv.createTemporaryView("abc", s2);  //注册了sql表名，后续可以用sql语句查询
//        Table table = tenv.fromDataStream(s2);  //得到table对象，后续可以用api进行查询

        //做查询：每种性别中年龄最大的3个人信息
        String sql1 =
                "       SELECT         \n" +
                        "   id,        \n" +
                        "   name,      \n" +
                        "   age,       \n" +
                        "   gender,    \n" +
                        "   rn         \n" +
                        "FROM          \n" +
                        "(             \n" +
                        "   select     \n" +
                        "       id,    \n" +
                        "       name,  \n" +
                        "       age,   \n" +
                        "       gender,\n" +
                        "       row_number() over(partition by gender order by age desc) as rn\n" +
                        "   from abc   \n" +
                        ") o           \n" +
                        "where rn<= 3  \n";

        /**
         * topN 的查询结果，创建为视图，继续查询
         * 方式 一
         */
        Table tmp = tenv.sqlQuery(sql1);
        tenv.createTemporaryView("tmp", tmp);
//        tenv.executeSql("select * from tmp where age % 2 = 0").print();
//        +----+-------------+--------------------------------+-------------+--------------------------------+----------------------+
//        | op |          id |                           name |         age |                         gender |                   rn |
//        +----+-------------+--------------------------------+-------------+--------------------------------+----------------------+
//        | +I |           1 |                             zs |          18 |                           male |                    1 |


        /**
         * topN 的查询结果，创建为视图，继续查询
         * 方式 二
         */
        String sql2 = "create temporary view topN_view as                              \n" +
                "   SELECT                                                             \n" +
                "       id,                                                            \n" +
                "       name,                                                          \n" +
                "       age,                                                           \n" +
                "       gender,                                                        \n" +
                "       rn                                                             \n" +
                "   FROM                                                               \n" +
                "   (                                                                  \n" +
                "   select                                                             \n" +
                "       id,                                                            \n" +
                "       name,                                                          \n" +
                "       age,                                                           \n" +
                "       gender,                                                        \n" +
                "       row_number() over(partition by gender order by age desc) as rn \n" +
                "   from abc                                                           \n" +
                "   ) o                                                                \n" +
                "   where rn <= 3";

        tenv.executeSql(sql2);
        tenv.executeSql("create temporary view topN_odd as select * from topN_view where age % 2 = 1");

        // 创建目标 kafka 映射表
        tenv.executeSql(
                " create table t_upsert_kafka2 (                      "
                        + "    id int,                                         "
                        + "    name string,                                    "
                        + "    age int,                                        "
                        + "    gender string,                                  "
                        + "    rn bigint,                                      "
                        + "    primary key(gender, rn) NOT ENFORCED            "
                        + " ) with (                                           "
                        + "  'connector' = 'upsert-kafka',                     "
                        + "  'topic' = 'flinksql_20',                          "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092',"
                        + "  'key.format' = 'csv',                             "
                        + "  'value.format' = 'csv'                            "
                        + " )                                                  "
        );

        tenv.executeSql("insert into t_upsert_kafka2 select * from topN_odd");

//        tenv.executeSql("select gender, max(age) as max_age from t_upsert_kafka2 group by gender").print();
        //+----+--------------------------------+-------------+
        //| op |                         gender |     max_age |
        //+----+--------------------------------+-------------+
        //| +I |                           male |          17 |

        // 将上述查询结果变成流
        DataStream<Row> dataStream = tenv.toChangelogStream(tenv.from("topN_odd"));

        // 打印流
        dataStream.print("1");
//        +----+--------------------------------+-------------+
//        | op |                         gender |     max_age |
//        +----+--------------------------------+-------------+
//        | +I |                           male |          17 |
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {
        private int id;
        private String name;
        private String gender;
        private int age;
    }
}
