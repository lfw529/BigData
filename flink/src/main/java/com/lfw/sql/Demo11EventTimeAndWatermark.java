package com.lfw.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * watermark 在 DDL 中的定义示例代码
 *
 * 测试数据：
 *     {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e05","eventTime":1655017436000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e06","eventTime":1655017437000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e07","eventTime":1655017438000,"pageId":"p001"}
 *     {"guid":1,"eventId":"e08","eventTime":1655017439000,"pageId":"p001"}
 */
public class Demo11EventTimeAndWatermark {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        /**
         * 注意：
         *  1.只有 TIMESTAMP 或 TIMESTAMP_LTZ 类型的字段可以被声明为 rowtime (事件时间属性), bigint 类型不能声明为 rowtime
         *  2.一个表可以既有事件时间属性和处理时间属性
         */
        tenv.executeSql(
                 "create table t_events (                                          "
                        + "   guid int,                                                     "
                        + "   eventId string,                                               "
                        /*+ "   eventTime timestamp(3),                                     "*/
                        + "   eventTime bigint,                                             "
                        + "   pageId string,                                                "
                        + "   pt AS proctime(),                                             "  // 利用一个表达式字段，来声明 processing time属性
                        + "   rt as to_timestamp_ltz(eventTime, 3),                         "  // 将 eventTime 转化为 TIMESTAMP_LTZ 类型来用作定义事件时间
                        + "   watermark for rt  as rt - interval '0.001' second             "  // 用watermark for xxx，来将一个已定义的TIMESTAMP/TIMESTAMP_LTZ字段声明成 eventTime属性及指定watermark策略
                        + " )                                                               "
                        + " with (                                                          "
                        + "   'connector' = 'kafka',                                        "
                        + "   'topic' = 'flinksql-11',                                      "
                        + "   'properties.bootstrap.servers' = 'hadoop102:9092',            "
                        + "   'properties.group.id' = 'g1',                                 "
                        + "   'scan.startup.mode' = 'earliest-offset',                      "
                        + "   'format' = 'json',                                            "
                        + "   'json.fail-on-missing-field' = 'false',                       "
                        + "   'json.ignore-parse-errors' = 'true'                           "
                        + " )                                                               "
        );

        tenv.executeSql("desc t_events").print();
        //验证是否能获取到当前动态表 watermark
        tenv.executeSql("select guid, eventId, eventTime, pageId, pt, rt, CURRENT_WATERMARK(rt) as wm from t_events").print();
    }
}
