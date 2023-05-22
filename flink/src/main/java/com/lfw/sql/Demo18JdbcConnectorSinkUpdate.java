package com.lfw.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo18JdbcConnectorSinkUpdate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);

        // 建表来映射 mysql 中的 flinksql.stu
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

        //输入数据：15,male
        SingleOutputStreamOperator<Bean1> bean1 = env.socketTextStream("hadoop102", 9999).map(s -> {
            String[] arr = s.split(",");
            return new Bean1(Integer.parseInt(arr[0]), arr[1]);
        });
        //输入数据：15,zs,18
        SingleOutputStreamOperator<Bean2> bean2 = env.socketTextStream("hadoop102", 8888).map(s -> {
            String[] arr = s.split(",");
            return new Bean2(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2]));
        });

        //流转表
        tenv.createTemporaryView("bean1", bean1);
        tenv.createTemporaryView("bean2", bean2);

        //保证顺序一致性
        tenv.executeSql("insert into flink_stu " +
                "select bean1.id, bean2.name, bean2.age, bean1.gender from bean1 left join bean2 on bean1.id = bean2.id");

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean1 {
        public int id;
        public String gender;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean2 {
        public int id;
        public String name;
        public int age;
    }
}

