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

/**
 * 时态join代码示例
 * <p>
 * 说明：永远只关联流中时间对应数据库表中最新的记录。
 * 例如：
 * 数据库中数据为：e,1.5,2000
 * 流中的数据为：e,2.0,2000 可以关联上
 * 流中的数据为：e,2.0,1500 则不可以关联上
 *
 * <p>
 * mysql 建表测试
 * CREATE TABLE `currency_rate`
 * (
 * `currency`        varchar(255) NOT NULL COMMENT '货币类型',
 * `rate`            double COMMENT '汇率',
 * `update_time`     bigint COMMENT '更新时间',
 * PRIMARY KEY (`currency`)
 * ) ENGINE = InnoDB DEFAULT CHARSET = utf8 COMMENT ='currency_rate表';
 * <p>
 * -- 插入语句
 * insert into currency_rate value('e', 1.5, 2000);
 * <p>
 * System.exit(0) ：status 是零参数，那么表示正常退出程序。
 * System.exit(1) ：status 是1或者非零参数，那么表示非正常退出程序。
 */
public class Demo26TemporalJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         * 订单Id，币种，金额，订单时间
         *
         * 端口号输入测试数据：
         * 1,e,100,1500
         * 1,e,200,2000
         */
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Order> ss1 = s1.map(s -> {
            String[] arr = s.split(",");
            return new Order(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]), Long.parseLong(arr[3]));
        });


        // 创建主表（需要声明处理时间属性字段）
        tenv.createTemporaryView("orders", ss1, Schema.newBuilder()
                .column("orderId", DataTypes.INT())
                .column("currency", DataTypes.STRING())
                .column("price", DataTypes.DOUBLE())
                .column("orderTime", DataTypes.BIGINT())
                .columnByExpression("rt", "to_timestamp_ltz(orderTime, 3)")  // 定义处理时间属性字段
                .watermark("rt", "rt")
                .build());

//        tenv.executeSql("select orderId, currency, price, orderTime, rt from orders").print();

        // 创建 temporal 表
        tenv.executeSql("CREATE TABLE currency_rate (           \n" +
                "      currency STRING,                                  \n" +
                "      rate double,                                      \n" +
                "      update_time bigint,                               \n" +
                "      rt as to_timestamp_ltz(update_time, 3),           \n" +
                "      watermark for rt as rt - interval '0' second,     \n" +
                "      PRIMARY KEY(currency) NOT ENFORCED                \n" +
                "     ) WITH (                                           \n" +
                "     'connector' = 'mysql-cdc',                         \n" +
                "     'hostname' = 'hadoop102',                          \n" +
                "     'port' = '3306',                                   \n" +
                "     'username' = 'root',                               \n" +
                "     'password' = '1234',                               \n" +
                "     'database-name' = 'flinksql',                      \n" +
                "     'table-name' = 'currency_rate'                     \n" +
                ")");

//        tenv.executeSql("select * from currency_rate").print();
//        System.exit(1);

        // temporal 关联查询，注意：order 是关键字，所以用 orders
        tenv.executeSql(
                "SELECT                   \n" +
                        "     orders.orderId,      \n" +
                        "     orders.currency,     \n" +
                        "     orders.price,        \n" +
                        "     orders.orderTime,    \n" +
                        "     rate                 \n" +
                        "FROM orders               \n" +
                        "LEFT JOIN currency_rate FOR SYSTEM_TIME AS OF orders.rt  \n" +
                        "ON orders.currency = currency_rate.currency"
        ).print();
        //关联结果如下
//+----+-------------+--------------------------------+--------------------------------+----------------------+--------------------------------+
//| op |     orderId |                       currency |                          price |            orderTime |                           rate |
//+----+-------------+--------------------------------+--------------------------------+----------------------+--------------------------------+
//| +I |           1 |                              e |                          100.0 |                 1500 |                         (NULL) |
//| +I |           1 |                              e |                          200.0 |                 2000 |                            1.5 |

        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order {
        // 订单Id，币种，金额，订单时间
        public int orderId;
        public String currency;
        public double price;
        public long orderTime;
    }
}
