package com.lfw.sql;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 流 ===> 表，过程中如何传承事件时间和 watermark
 * 测试数据：
 *   {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
 *   {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
 *   {"guid":1,"eventId":"e04","eventTime":1655017435000,"pageId":"p001"}
 */
public class Demo12EventTimeAndWatermarkForStreamToTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        // {"guid":1,"eventId":"e02","eventTime":1655017433000,"pageId":"p001"}
        // {"guid":1,"eventId":"e03","eventTime":1655017434000,"pageId":"p001"}
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Event> s2 = s1.map(s -> JSON.parseObject(s, Event.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.eventTime;
                            }
                        })
                );

        //观察流上的watermark推进
       /* s2.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                long wm = ctx.timerService().currentWatermark();
                out.collect(value + " => " + wm);
            }
        }).print();*/

        //这样，直接把流转成表，会丢失 watermark
        tenv.createTemporaryView("t_events", s2);

        //报错：CURRENT_WATERMARK() must be called with a single rowtime attribute argument, but 'BIGINT NOT NULL' cannot be a time attribute.
        /*tenv.executeSql("select guid, eventId, eventTime, pageId, current_watermark(eventTime) from t_events").print();*/

        //测试验证 watermark 的丢失
        Table table = tenv.from("t_events");
        DataStream<Row> ds = tenv.toDataStream(table);
        ds.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(row + " => " + ctx.timerService().currentWatermark());
            }
        })/*.print()*/;  //打印的时候发现 watermark 已经丢失
        //+I[1, e02, 1655017433000, p001] => -9223372036854775808
        //+I[1, e03, 1655017434000, p001] => -9223372036854775808
        //+I[1, e04, 1655017435000, p001] => -9223372036854775808

        // 可以在流转表时，显式声明 watermark 策略
        tenv.createTemporaryView("t_events_wm", s2, Schema.newBuilder()
                .column("guid", DataTypes.INT())
                .column("eventId", DataTypes.STRING())
                .column("eventTime", DataTypes.BIGINT())
                .column("pageId", DataTypes.STRING())

                //方式1：通过声明表达式字段，将eventTime转为事件时间属性
                .columnByExpression("rt", "to_timestamp_ltz(eventTime, 3)")  //重新利用一个bigint转成timestamp后，作为事件时间属性
                //方式2：通过元数据获取
//                .columnByMetadata("rt", DataTypes.TIMESTAMP_LTZ(3), "rowtime")  //利用底层流连接器暴露的rowtime元数据(代表的就是底层流中每条数据上的 eventTime)，声明成事件时间属性字段

                //方式1：重新定义watermark策略
//                .watermark("rt", "rt - interval '1' second ")  //重新定义表上的watermark策略
                //方式2：引入流中的watermark
                .watermark("rt", "source_watermark()") //声明watermark直接引用底层流的watermark
                .build());

        tenv.executeSql("select guid, eventId, eventTime, pageId, rt, current_watermark(rt) as wm from t_events_wm").print();
//        +----+-------------+--------------------------------+----------------------+--------------------------------+-------------------------+-------------------------+
//        | op |        guid |                        eventId |            eventTime |                         pageId |                      rt |                      wm |
//        +----+-------------+--------------------------------+----------------------+--------------------------------+-------------------------+-------------------------+
//        | +I |           1 |                            e02 |        1655017433000 |                           p001 | 2022-06-12 15:03:53.000 |                  (NULL) |
//        | +I |           1 |                            e03 |        1655017434000 |                           p001 | 2022-06-12 15:03:54.000 | 2022-06-12 15:03:51.999 |
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Event {
        public int guid;
        public String eventId;
        public long eventTime;
        public String pageId;
    }
}
