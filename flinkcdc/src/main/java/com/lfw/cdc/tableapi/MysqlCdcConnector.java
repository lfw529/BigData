package com.lfw.cdc.tableapi;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MysqlCdcConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///e:/checkpoint");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 建表
        tenv.executeSql("CREATE TABLE flink_score (\n" +
                "      id int,                              \n" +
                "      name string,                         \n" +
                "      gender string,                       \n" +
                "      score double,                        \n" +
                "      PRIMARY KEY(id) NOT ENFORCED         \n" +
                "    ) WITH (                               \n" +
                "     'connector' = 'mysql-cdc',            \n" +
                "     'hostname' = 'hadoop102',             \n" +
                "     'port' = '3306',                      \n" +
                "     'username' = 'root',                  \n" +
                "     'password' = '1234',                  \n" +
                "     'database-name' = 'flinksql',         \n" +
                "     'table-name' = 'stu_score'            \n" +
                ")");

        // 查询
//        tenv.executeSql("select * from flink_score")/*.print()*/;
//        +----+-------------+--------------------------------+--------------------------------+--------------------------------+
//        | op |          id |                           name |                         gender |                          score |
//        +----+-------------+--------------------------------+--------------------------------+--------------------------------+
//        | +I |           1 |                             zs |                           male |                           77.8 |
//        | +I |           2 |                             ls |                           male |                           66.3 |
//        | +I |           3 |                             ww |                         female |                           55.1 |
//        | +I |           4 |                             zl |                           male |                           92.8 |
//        | +I |           5 |                             tq |                         female |                           83.2 |
//        | +I |           6 |                             po |                         female |                           43.5 |
//        | +I |           7 |                             xx |                         female |                           35.8 |
//        | +I |           8 |                             yy |                           male |                           79.8 |

        //聚合查询
//        tenv.executeSql("select gender, avg(score) as avg_score from flink_score group by gender")/*.print()*/;

        //建一个目标表，对应 mysql 中的 score_rank，用来存放查询结果：每种性别中，总分最高的前2个人。
        tenv.executeSql(
                "create table flink_rank (                        \n" +
                        "   gender string,                                 \n" +
                        "   name string,                                   \n" +
                        "   score_amt double,                              \n" +
                        "   rn bigint,                                     \n" +
                        "   primary key(gender, rn) not enforced           \n" +
                        ") with (                                          \n" +
                        "  'connector' = 'jdbc',                           \n" +
                        "  'url' = 'jdbc:mysql://hadoop102:3306/flinksql', \n" +
                        "  'table-name' = 'score_rank',                    \n" +
                        "  'username' = 'root',                            \n" +
                        "  'password' = '1234'                             \n" +
                        ")"
        );


        tenv.executeSql("insert into flink_rank  \n" +
                "SELECT\n" +
                "  gender,\n" +
                "  name,\n" +
                "  score_amt,\n" +
                "  rn\n" +
                "from(\n" +
                "SELECT\n" +
                "  gender,\n" +
                "  name,\n" +
                "  score_amt,\n" +
                "  row_number() over(partition by gender order by score_amt desc) as rn\n" +
                "from \n" +
                "(\n" +
                "SELECT\n" +
                "gender,\n" +
                "name,\n" +
                "sum(score) as score_amt\n" +
                "from flink_score\n" +
                "group by gender,name\n" +
                ") o1\n" +
                ") o2\n" +
                "where rn<=2").print();
    }
}
