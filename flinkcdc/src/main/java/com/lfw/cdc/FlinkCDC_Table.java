package com.lfw.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC_Table {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用 flinkcdc sql 方式建表
        tableEnv.executeSql("create table t1 (\n" +
                "id string primary key not enforced,\n" +
                "name string\n" +
                ")\n" +
                "with ( \n" +
                "'connector' = 'mysql-cdc',\n" +
                "'hostname' = 'hadoop102',\n" +
                "'port' = '3306',\n" +
                "'username' = 'root',\n" +
                "'password' = '1234',\n" +
                "'database-name' = 'flinkcdc',\n" +
                "'table-name' = 't1'\n" +
                ")");

        tableEnv.executeSql("select * from t1").print();

        env.execute();
    }
}
