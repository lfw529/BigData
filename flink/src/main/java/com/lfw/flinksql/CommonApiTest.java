package com.lfw.flinksql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CommonApiTest {
    public static void main(String[] args) {
        //方式一：
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1 创建表环境
        //1.1 方式二：定义环境配置来创建表执行环境
        EnvironmentSettings settings1 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv1 = TableEnvironment.create(settings1);

        //1.2 方式三：定义环境配置来创建表执行环境 [基于 Blink planner 进行流处理]
        EnvironmentSettings settings2 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);

        //1.3 方式四：定义环境配置来创建表执行环境 [基于 Blink planner 进行批处理]
        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
                .inBatchMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv3 = TableEnvironment.create(settings3);

        //1.4 方式五：定义环境配置来创建表执行环境 [基于老版本 planner 进行流处理]  [移除]
//        EnvironmentSettings settings4 = EnvironmentSettings.newInstance()
//                .inStreamingMode()
//                .useOldPlanner()
//                .build();

//        TableEnvironment tableEnv4 = TableEnvironment.create(settings4);

        //1.5 方式六：基于老版本 planner 进行批处理 [已经移除]

        //2 创建表
        String createDDL = "CREATE TABLE clickTable (" +
                " user_name STRING, " +
                " url STRING, " +
                " ts BIGINT " +
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = 'E:\\IdeaProject\\bigdata\\flink\\input\\click.txt'," +
                " 'format' = 'csv'" +
                ")";

        tableEnv1.executeSql(createDDL);

        //3.表的查询
        //3.1调用Table API进行表的查询转换
        Table clickTable = tableEnv1.from("clickTable");
        Table resultTable = clickTable.where($("user_name").isEqual("Bob"))
                .select($("user_name"),$("url"));

        tableEnv1.createTemporaryView("resultTable", resultTable);

        //执行SQL进行表的查询转换
        Table resultTable2 = tableEnv1.sqlQuery("select url, user_name from resultTable");

        //执行聚合计算的查询转换
        Table aggResult = tableEnv1.sqlQuery("select user_name, count(url) as cnt from clickTable group by user_name");

        //创建一张用于输出的表
        String createOutDDL = "CREATE TABLE outTable (" +
                " user_name STRING, " +
                " url STRING " +
                " ) WITH (" +
                "   'connector' = 'filesystem'," +
                "   'path' = 'E:\\IdeaProject\\bigdata\\flink\\output'," +
                "   'format' = 'csv'" +
                " )";

        tableEnv1.executeSql(createOutDDL);

        //创建一张用于控制台打印输出的表
        String createPrintOutDDL = "CREATE TABLE printOutTable (" +
                " user_name STRING, " +
                " url STRING " +
                " ) WITH (" +
                "   'connector' = 'print'" +
                " )";

        //创建一张用于控制台打印输出的表
        String createPrintOutDDL2 = "CREATE TABLE printOutTable2 (" +
                " user_name STRING, " +
                " url_count BIGINT " +
                " ) WITH (" +
                "   'connector' = 'print'" +
                " )";

        tableEnv1.executeSql(createPrintOutDDL);
        //输出表
        resultTable.executeInsert("outTable");
        resultTable2.executeInsert("printOutTable");

        System.out.println("----------------");
        tableEnv1.executeSql(createPrintOutDDL2);
        aggResult.executeInsert("printOutTable2");
    }
}
