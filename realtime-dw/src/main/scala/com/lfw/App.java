package com.lfw;

import com.lfw.sql.OdsOrderInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class App {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //环境创建之初，底层会根据自动初始化一个元数据空间实现对象 (default_catalog => GenericInMemoryCatalog)
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(OdsOrderInfo.source());
        tableEnv.executeSql("select * from ods_order_detail").print();
//        tableEnv.executeSql(OdsOrderInfo.sink());
//        tableEnv.executeSql(OdsOrderInfo.transform());

    }
}
