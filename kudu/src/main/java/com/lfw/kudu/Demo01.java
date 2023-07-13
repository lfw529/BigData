package com.lfw.kudu;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings environmentSettings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, environmentSettings);

        tenv.executeSql(
                "create table t_kudu                                         \n" +
                        "    id BIGINT,   \n" +
                        "    end_time STRING,   \n" +
                        "    start_time STRING,   \n" +
                        "    transferindes STRING,   \n" +
                        "    transferoutdes STRING,   \n" +
                        "    valid BIGINT,   \n" +
                        "    version BIGINT,   \n" +
                        "    position_id BIGINT,   \n" +
                        "    staff_id BIGINT,   \n" +
                        "    mainposiflag BIGINT,   \n" +
                        "    parentpwid BIGINT,   \n" +
                        "    editdate STRING,   \n" +
                        "    edit_date STRING,   \n" +
                        "    main_posi_flag BIGINT,   \n" +
                        "    parent_pw_id BIGINT,   \n" +
                        "    transfer_in_des STRING,   \n" +
                        "    transfer_out_des STRING,   \n" +
                        "    transfer_in_deal_time STRING,   \n" +
                        "    transfer_out_deal_time STRING,   \n" +
                        "    transfer_in_dealer_id BIGINT,   \n" +
                        "    transfer_out_dealer_id BIGINT,   " +
                        ") WITH (\n" +
                        "  'connector.type' = 'kudu',\n" +
                        "  'kudu.masters' = '10.30.250.21:7051',\n" +
                        "  'kudu.table' = 'rods.rods_bpm_grouphr_base_positionwork_f_kudu',\n" +
                        "  'kudu.hash-columns' = 'id',\n" +
                        "  'kudu.primary-key-columns' = 'id'\n" +
                        ")"
        );

        // 用 tableSql 查询计算结果
        tenv.executeSql("select * from t_kudu").print();
    }
}