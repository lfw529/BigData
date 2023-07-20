package com.lfw.kudu.supcon.utils;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.util.concurrent.TimeUnit;

@Slf4j
public final class HiveConnect {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tableEnv;

    public HiveConnect() {
        initEnv();
    }

    private void initEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);
        env.getCheckpointConfig().setCheckpointInterval(30000L);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1000);
        env.getCheckpointConfig().setCheckpointTimeout(60000 * 10L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(50, TimeUnit.SECONDS.toSeconds(10)));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "kudu/src/main/resources/";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        this.tableEnv = tableEnv;
    }

    /**
     * 程序执行的入口,用来处理业务逻辑
     *
     * @param sql 这是一个接口, 业务开发只需要关注自己的业务逻辑sql, 开发时, 只需要调用
     *                这个方法, 饭后执行后续的逻辑即可
     */
    public void process(String sql) {
        try {
            tableEnv.executeSql(sql).print();
        } catch (Exception e) {
            log.error("this job has canceled because {}", e.getMessage());
            e.printStackTrace();
        }
    }
}
