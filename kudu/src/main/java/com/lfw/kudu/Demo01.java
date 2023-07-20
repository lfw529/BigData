package com.lfw.kudu;

import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //环境创建之初，底层会根据自动初始化一个元数据空间实现对象 (default_catalog => GenericInMemoryCatalog)
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

//        String KUDU_MASTER = "bdmaster-t-01:7051, bdmaster-t-02:7051";
//        KuduCatalog catalog = new KuduCatalog(KUDU_MASTER);
//
//        tenv.registerCatalog("Kudu", catalog);
//
//        tenv.useCatalog("Kudu");
//

        tenv.executeSql(
                "CREATE TABLE cdc_test1 (\n" +
                        "  id int,\n" +
                        "  val1 STRING,\n" +
                        "  num_val STRING,\n" +
                        "  time_val Timestamp(3)\n" +
                        ") WITH (\n" +
                        "  'connector.type' = 'kudu',\n" +
                        "  'kudu.masters' = 'bdmaster-t-01:7051, bdmaster-t-02:7051',\n" +
                        "  'kudu.table' = 'rods.cdc_test',\n" +
                        "  'kudu.primary-key-columns' = 'id'\n" +
                        ")"
        );

        // 用 tableSql 查询计算结果
        tenv.executeSql("select * from cdc_test1").print();
    }
}