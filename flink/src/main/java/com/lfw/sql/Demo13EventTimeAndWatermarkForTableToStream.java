package com.lfw.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * 表 ===> 流，过程中如何传承事件时间和 watermark
 * 测试数据：[一条一条输入]
 *   {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
 *   {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
 *   {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
 */
public class Demo13EventTimeAndWatermarkForTableToStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        tenv.executeSql(
                "create table t_events (                                          "
                        + "   guid int,                                                     "
                        + "   eventId string,                                               "
                        + "   eventTime bigint,                                             "
                        + "   pageId string,                                                "
                        /*+ "   pt AS proctime(),                                             "*/  // 利用一个表达式字段，来声明 processing time属性
                        + "   rt as to_timestamp_ltz(eventTime, 3),                         "
                        + "   watermark for rt  as rt - interval '1' second                 "  // 用watermark for xxx，来将一个已定义的TIMESTAMP/TIMESTAMP_LTZ字段声明成 eventTime属性及指定watermark策略
                        + " )                                                               "
                        + " with (                                                          "
                        + "   'connector' = 'kafka',                                        "
                        + "   'topic' = 'flinksql-13',                                      "
                        + "   'properties.bootstrap.servers' = 'hadoop102:9092',            "
                        + "   'properties.group.id' = 'g1',                                 "
                        + "   'scan.startup.mode' = 'earliest-offset',                      "
                        + "   'format' = 'json',                                            "
                        + "   'json.fail-on-missing-field' = 'false',                       "
                        + "   'json.ignore-parse-errors' = 'true'                           "
                        + " )                                                               "
        );

//        tenv.executeSql("select guid, eventId, rt, current_watermark(rt) as wm from t_events").print();
//        +----+-------------+--------------------------------+-------------------------+-------------------------+
//        | op |        guid |                        eventId |                      rt |                      wm |
//        +----+-------------+--------------------------------+-------------------------+-------------------------+
//        | +I |           1 |                            e02 | 2022-06-12 15:03:53.000 |                  (NULL) |
//        | +I |           1 |                            e03 | 2022-06-12 15:03:54.000 | 2022-06-12 15:03:52.000 |
//        | +I |           1 |                            e04 | 2022-06-12 15:03:55.000 | 2022-06-12 15:03:53.000 |

        // 自动传递 watermark
        DataStream<Row> ds = tenv.toDataStream(tenv.from("t_events"));
        ds.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value + " => " + ctx.timerService().currentWatermark());
            }
        }).print();
        //+I[1, e02, 1655017433000, p001, 2022-06-12T07:03:53Z] => -9223372036854775808
        //+I[1, e03, 1655017434000, p001, 2022-06-12T07:03:54Z] => 1655017432000
        //+I[1, e04, 1655017435000, p001, 2022-06-12T07:03:55Z] => 1655017433000

        env.execute();
    }
}
