package com.lfw.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试数据：
 * 1,male,18
 * 2,male,28
 * 3,female,38
 * 4,male,20
 */
public class Demo15UpsertKafkaConnectorForSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 1,male,18
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Bean1> bean1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]));
        });

        // 流转表
        tenv.createTemporaryView("bean1", bean1);

        //tenv.executeSql("select gender, count(1) as cnt from bean1 group by gender").print();

        // 创建目标 kafka 映射表
        tenv.executeSql(
                " create table t_upsert_kafka (                      "
                        + "    gender string primary key not enforced,         "
                        + "    cnt bigint                                      "
                        + " ) with (                                           "
                        + "  'connector' = 'upsert-kafka',                     "
                        + "  'topic' = 'flinksql-15',                          "
                        + "  'properties.bootstrap.servers' = 'hadoop102:9092',"
                        + "  'key.format' = 'csv',                             "
                        + "  'value.format' = 'csv'                            "
                        + " )                                                  "

        );
        // 查询每种性别的数据行数，并将结果插入到目标表
        tenv.executeSql(
                "insert into t_upsert_kafka " +
                        "select gender, count(1) as cnt from bean1 group by gender"
        );

        tenv.executeSql("select * from t_upsert_kafka").print();
//        +----+--------------------------------+----------------------+
//        | op |                         gender |                  cnt |
//        +----+--------------------------------+----------------------+
//        | +I |                           male |                    1 |
//        | -U |                           male |                    1 |
//        | +U |                           male |                    2 |
//        | +I |                         female |                    1 |
//        | -U |                           male |                    2 |
//        | +U |                           male |                    3 |
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1 {
        public int id;
        public String gender;
        public int age;
    }
}
