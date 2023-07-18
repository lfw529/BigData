package com.lfw.kudu.supcon;

import com.lfw.kudu.supcon.sql.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(60000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(20, TimeUnit.SECONDS)));

        //环境创建之初，底层会根据自动初始化一个元数据空间实现对象 (default_catalog => GenericInMemoryCatalog)
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // StatementSet statementSet = tableEnv.createStatementSet();

        //测试通过
//        tableEnv.executeSql(Table_rods_bpm_grouphr_base_department_f_kudu.source());
//        tableEnv.executeSql(Table_rods_bpm_grouphr_base_department_f_kudu.query()).print();

        //测试通过
//        tableEnv.executeSql(Table_rods_bpm_grouphr_base_position_f_kudu.source());
//        tableEnv.executeSql(Table_rods_bpm_grouphr_base_position_f_kudu.query()).print();

        //测试通过
//        tableEnv.executeSql(Table_rods_bpm_grouphr_base_position_f_kudu.source());
//        tableEnv.executeSql(Table_rods_bpm_grouphr_base_position_f_kudu.query()).print();

        //测试通过
//        tableEnv.executeSql(Table_rods_bpm_grouphr_basicdata_nations_f_kudu.source());
//        tableEnv.executeSql(Table_rods_bpm_grouphr_basicdata_nations_f_kudu.query()).print();

        //测试通过
//        tableEnv.executeSql(Table_rods_bpm_grouphr_techcrm_region_contrasts_f_kudu.source());
//        tableEnv.executeSql(Table_rods_bpm_grouphr_techcrm_region_contrasts_f_kudu.query()).print();

        tableEnv.executeSql(Table_ods_dmp_bigdata_user_center_sys_role_user_f_1d.source());
        tableEnv.executeSql(Table_ods_dmp_bigdata_user_center_sys_role_user_f_1d.query()).print();
    }
}
