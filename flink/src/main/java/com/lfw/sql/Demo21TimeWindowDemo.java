package com.lfw.sql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class Demo21TimeWindowDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // bid_time | price | item | supplier_id |
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Bid> s2 = s1.map(s -> {
            String[] split = s.split(",");
            return new Bid(split[0], Double.parseDouble(split[1]), split[2], split[3]);
        });

        //把流变成表
        tenv.createTemporaryView("t_bid", s2, Schema.newBuilder()
                .column("bid_time", DataTypes.STRING())
                .column("price", DataTypes.DOUBLE())
                .column("item", DataTypes.STRING())
                .column("supplier_id", DataTypes.STRING())
                .columnByExpression("rt", $("bid_time").toTimestamp())
                .watermark("rt", "rt - interval '1' second")
                .build());

        // 查询
        // tenv.executeSql("select bid_time, price, item, supplier_id, current_watermark(rt) as wm from t_bid").print();

        //TODO 1: 每分钟，计算最近5分钟的交易总额
        //端口号输入测试数据：
//        2020-04-15 08:05:00.000,4.00,C,supplier1
//        2020-04-15 08:07:00.000,2.00,A,supplier1
//        2020-04-15 08:09:00.000,5.00,D,supplier2
//        2020-04-15 08:11:00.000,3.00,B,supplier2
        tenv.executeSql(
                "select                                                                         \n" +
                        "   window_start,                                                                \n" +
                        "   window_end,                                                                  \n" +
                        "   sum(price) as price_amt                                                      \n" +
                        " from table(                                                                    \n" +
                        "   hop(table t_bid, descriptor(rt), interval '1' minutes, interval '5' minutes) \n" +
                        ")                                                                               \n" +
                        "group by window_start, window_end"
        )/*.print()*/;
        //        +----+-------------------------+-------------------------+--------------------------------+
        //        | op |            window_start |              window_end |                      price_amt |
        //        +----+-------------------------+-------------------------+--------------------------------+
        //        | +I | 2020-04-15 08:01:00.000 | 2020-04-15 08:06:00.000 |                            4.0 |
        //        | +I | 2020-04-15 08:02:00.000 | 2020-04-15 08:07:00.000 |                            4.0 |
        //        | +I | 2020-04-15 08:03:00.000 | 2020-04-15 08:08:00.000 |                            6.0 |
        //        | +I | 2020-04-15 08:04:00.000 | 2020-04-15 08:09:00.000 |                            6.0 |
        //        | +I | 2020-04-15 08:05:00.000 | 2020-04-15 08:10:00.000 |                           11.0 |

        //TODO 2: 每2分钟计算最近2分钟的交易总额
        //端口号输入测试数据：
//        2020-04-15 08:05:00.000,4.00,C,supplier1
//        2020-04-15 08:07:00.000,2.00,A,supplier1
//        2020-04-15 08:09:00.000,5.00,D,supplier2
//        2020-04-15 08:11:00.000,3.00,B,supplier2
        tenv.executeSql(
                "select                                                           \n" +
                        "  window_start,                                                   \n" +
                        "  window_end,                                                     \n" +
                        "  sum(price) as price_amt                                         \n" +
                        "from table (                                                      \n" +
                        "  tumble(table t_bid, descriptor(rt), interval '2' minutes)       \n" +
                        ")                                                                 \n" +
                        "group by window_start, window_end"
        )/*.print()*/;
        //+----+-------------------------+-------------------------+--------------------------------+
        //| op |            window_start |              window_end |                      price_amt |
        //+----+-------------------------+-------------------------+--------------------------------+
        //| +I | 2020-04-15 08:04:00.000 | 2020-04-15 08:06:00.000 |                            4.0 |
        //| +I | 2020-04-15 08:06:00.000 | 2020-04-15 08:08:00.000 |                            2.0 |
        //| +I | 2020-04-15 08:08:00.000 | 2020-04-15 08:10:00.000 |                            5.0 |

        //TODO 3: 每2分钟计算今天以来的总交易额
        //端口号输入测试数据：
//        2020-04-15 08:05:00.000,4.00,C,supplier1
//        2020-04-15 08:07:00.000,2.00,A,supplier1
//        2020-04-15 08:09:00.000,5.00,D,supplier2
//        2020-04-15 08:11:00.000,3.00,B,supplier2
//
//        2020-04-15 08:13:00.000,1.00,E,supplier1
//        2020-04-15 08:17:00.000,6.00,F,supplier2
//
//        2020-04-16 08:17:00.000,100.00,F,supplier2
//        2020-04-16 08:20:00.000,200.00,F,supplier2
        tenv.executeSql(
                "select                                                                           \n" +
                        "   window_start,                                                                  \n" +
                        "   window_end,                                                                    \n" +
                        "   sum(price) as price_amt                                                        \n" +
                        "from table(                                                                       \n" +
                        "   cumulate(table t_bid, descriptor(rt), interval '2' minutes, interval '24' hour)\n" +
                        ")                                                                                 \n" +
                        "group by window_start, window_end"
        )/*.print()*/;

        //TODO 4: 每10分钟计算一次，最近10分钟内交易总额最大的前3个供应商及其交易单数
        //端口号输入测试数据：
//        2020-04-15 08:05:00.000,4.00,C,supplier1
//        2020-04-15 08:07:00.000,2.00,A,supplier1
//        2020-04-15 08:09:00.000,5.00,D,supplier2
//        2020-04-15 08:11:00.000,3.00,B,supplier2
//        2020-04-15 08:09:00.000,5.00,D,supplier3
//        2020-04-15 08:11:00.000,6.00,B,supplier3
//        2020-04-15 08:11:00.000,6.00,B,supplier3
        tenv.executeSql(
                "select\n" +
                        "  *\n" +
                        "from\n" +
                        "(\n" +
                        "   select\n" +
                        "       window_start, window_end,\n" +
                        "       supplier_id,\n" +
                        "       price_amt,\n" +
                        "       bid_cnt,\n" +
                        "       row_number() over(partition by window_start, window_end order by price_amt desc) as rn\n" +
                        "   from (\n" +
                        "       select\n" +
                    "               window_start,\n" +
                        "           window_end,\n" +
                        "           supplier_id,\n" +
                        "           sum(price) as price_amt,\n" +
                        "           count(1) as bid_cnt\n" +
                        "       from table(tumble(table t_bid, descriptor(rt), interval '10' minutes))\n" +
                        "       group by window_start, window_end, supplier_id\n" +
                        "   )\n" +
                        ") \n" +
                        "where rn<=2"
        )/*.print()*/;
        //+----+-------------------------+-------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
        //| op |            window_start |              window_end |                    supplier_id |                      price_amt |              bid_cnt |                   rn |
        //+----+-------------------------+-------------------------+--------------------------------+--------------------------------+----------------------+----------------------+
        //| +I | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier1 |                            6.0 |                    2 |                    1 |
        //| +I | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 |                      supplier2 |                            5.0 |                    1 |                    2 |

        //求每一个窗口中交易金额最大额两个定单
//        2020-04-15 08:05:00.000,4.00,C,supplier1
//        2020-04-15 08:07:00.000,2.00,A,supplier1
//        2020-04-15 08:09:00.000,5.00,D,supplier2
//        2020-04-15 08:11:00.000,3.00,B,supplier2
        tenv.executeSql("SELECT\n" +
                "  *\n" +
                "FROM \n" +
                "(\n" +
                "SELECT\n" +
                "   bid_time,\n" +
                "   price,\n" +
                "   item,\n" +
                "   supplier_id,\n" +
                "   row_number() over(partition by window_start, window_end order by price desc) as rn\n" +
                "FROM TABLE(TUMBLE(table t_bid, descriptor(rt), interval '10' minute))\n" +
                ")\n" +
                "WHERE rn<= 2").print();
//        +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
//        | op |                       bid_time |                          price |                           item |                    supplier_id |                   rn |
//        +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+----------------------+
//        | +I |        2020-04-15 08:09:00.000 |                            5.0 |                              D |                      supplier2 |                    1 |
//        | +I |        2020-04-15 08:05:00.000 |                            4.0 |                              C |                      supplier1 |                    2 |

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bid {
        private String bid_time;    // "2020-04-15 08:05:00.000"
        private double price;
        private String item;
        private String supplier_id;
    }
}
