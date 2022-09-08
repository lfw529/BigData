package com.lfw.hudi.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class InsertTest {
    public static void main(String[] args) {

        //1.获取表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        tableEnv.executeSql("CREATE TABLE table_hudi(\n" +
                "  uuid VARCHAR(20), \n" +
                "  name VARCHAR(10),\n" +
                "  age INT,\n" +
                "  ts TIMESTAMP(3),\n" +
                "  `partition` VARCHAR(20)\n" +
                ")\n" +
                "PARTITIONED BY (`partition`)\n" +
                "WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = 'hdfs://hadoop102:8020/table_hudi', \n" +
                "    'table.type' = 'COPY_ON_WRITE',\n" +
                "    'write.tasks' = '1',\n" +
                "    'compaction.tasks' = '1',\n" +
                "    'hive_sync.enable'='true',\n" +
                "    'hive_sync.table'='table_hudi',\n" +
                "    'hive_sync.db'='default',\n" +
                "    'hive_sync.mode' = 'hms',\n" +
                "    'hive_sync.metastore.uris' = 'thrift://hadoop102:9083'\n" +
                ")"
        );
        tableEnv.executeSql("INSERT INTO table_hudi VALUES\n" +
                "('id1','Danny',23,TIMESTAMP '1970-01-01 00:00:01','par1'),\n" +
                "('id2','Stephen',33,TIMESTAMP '1970-01-01 00:00:02','par1'),\n" +
                "('id3','Julian',53,TIMESTAMP '1970-01-01 00:00:03','par2'),\n" +
                "('id4','Fabian',31,TIMESTAMP '1970-01-01 00:00:04','par2'),\n" +
                "('id5','Sophia',18,TIMESTAMP '1970-01-01 00:00:05','par3'),\n" +
                "('id6','Emma',20,TIMESTAMP '1970-01-01 00:00:06','par3'),\n" +
                "('id7','Bob',44,TIMESTAMP '1970-01-01 00:00:07','par4'),\n" +
                "('id8','Han',56,TIMESTAMP '1970-01-01 00:00:08','par4')\n");
    }
}

