package com.lfw.sql;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 一条流的时间，取关联另一条流的一个时间范围
 * 时间范围 (interval) 限制方式
 * ltime = rtime  两边时间相等
 * ltime >= rtime AND ltime < rtime + INTERVAL '10' MINUTE    ==> ltime 属于 [rtime - 10min, rtime)
 * ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND ==> ltime 属于 [rtime - 5s, rtime + 10s]
 */
public class Demo24IntervalJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        //设置table环境中的状态ttl时长
        tenv.getConfig().getConfiguration().setLong("table.exec.state.ttl", 60 * 60 * 1000L);

        /**
         * 1,a,1000
         * 2,b,2000
         * 3,c,2500
         * 4,d,3000
         * 5,e,12000
         */
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {
        });

        /**
         * 1,bj,1000
         * 2,sh,2100
         * 4,xa,2600
         * 5,yn,14000
         */
        DataStreamSource<String> s2 = env.socketTextStream("hadoop102", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss2 = s2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
        }).returns(new TypeHint<Tuple3<String, String, Long>>() {
        });


        // 创建两个表
        tenv.createTemporaryView("t_left", ss1, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '0' second")
                .build());

        tenv.createTemporaryView("t_right", ss2, Schema.newBuilder()
                .column("f0", DataTypes.STRING())
                .column("f1", DataTypes.STRING())
                .column("f2", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(f2, 3)")
                .watermark("rt", "rt - interval '0' second")
                .build());

        //interval join between 写法
        tenv.executeSql("select a.f0, a.f1, a.f2, b.f0, b.f1 from t_left a join t_right b " +
                "on a.f0 = b.f0 " +
                "and a.rt between b.rt - interval '2' second and b.rt")/*.print()*/;
//                +----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
//                | op |                             f0 |                             f1 |                   f2 |                            f00 |                            f10 |
//                +----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
//                | +I |                              1 |                              a |                 1000 |                              1 |                             bj |
//                | +I |                              2 |                              b |                 2000 |                              2 |                             sh |
//                | +I |                              5 |                              e |                12000 |                              5 |                             yn |

        //interval join >=< 写法
        tenv.executeSql("select a.f0, a.f1, a.f2, b.f0, b.f1 from t_left a join t_right b " +
                "on a.f0 = b.f0 " +
                "and a.rt >= b.rt - interval '2' second and a.rt < b.rt").print();
//        +----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
//        | op |                             f0 |                             f1 |                   f2 |                            f00 |                            f10 |
//        +----+--------------------------------+--------------------------------+----------------------+--------------------------------+--------------------------------+
//        | +I |                              2 |                              b |                 2000 |                              2 |                             sh |
//        | +I |                              5 |                              e |                12000 |                              5 |                             yn |
    }
}
