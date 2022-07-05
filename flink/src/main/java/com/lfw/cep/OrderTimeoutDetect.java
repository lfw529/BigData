package com.lfw.cep;

import com.lfw.pojo.OrderCEP;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class OrderTimeoutDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取订单事件流，并提取时间戳、生成水位线
        KeyedStream<OrderCEP, String> stream = env
                .fromElements(
                        new OrderCEP("user_1", "order_1", "create", 1000L),
                        new OrderCEP("user_2", "order_2", "create", 2000L),
                        new OrderCEP("user_1", "order_1", "modify", 10 * 1000L),
                        new OrderCEP("user_1", "order_1", "pay", 60 * 1000L),
                        new OrderCEP("user_2", "order_3", "create", 10 * 60 * 1000L),
                        new OrderCEP("user_2", "order_3", "pay", 20 * 60 * 1000L)
                ).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderCEP>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderCEP>() {
                                            @Override
                                            public long extractTimestamp(OrderCEP event, long l) {
                                                return event.timestamp;
                                            }
                                        }
                                )
                )
                .keyBy(order -> order.orderId);    // 按照订单ID分组

        //1.定义 Pattern
        Pattern<OrderCEP, ?> pattern = Pattern
                .<OrderCEP>begin("create")   //首先是下单事件
                .where(new SimpleCondition<OrderCEP>() {
                    @Override
                    public boolean filter(OrderCEP value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .followedBy("pay")  //之后是支付事件；中间可以修改订单，宽松近邻
                .where(new SimpleCondition<OrderCEP>() {
                    @Override
                    public boolean filter(OrderCEP value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.minutes(15));  //限制在15分钟之内

        //2. 将 Pattern 应用到流上，检测匹配的复杂事件，得到一个 PatternStream
        PatternStream<OrderCEP> patternStream = CEP.pattern(stream, pattern);

        //3.定义一个测输出流标签
        OutputTag<String> timeoutTag = new OutputTag<String>("timeout"){};

        //4.将匹配到的，和超时部分匹配的复杂事件提取出来，包装成提示信息输出
        SingleOutputStreamOperator<String> payedOrderStream = patternStream.process(new OrderPayPatternProcessFunction());

        //将正常匹配和超时部分匹配的处理结果流打印输出
        payedOrderStream.print("payed");
        payedOrderStream.getSideOutput(timeoutTag).print("timeout");

        env.execute();
    }

    // 实现自定义的PatternProcessFunction，需实现TimedOutPartialMatchHandler接口
    public static class OrderPayPatternProcessFunction extends PatternProcessFunction<OrderCEP, String> implements TimedOutPartialMatchHandler<OrderCEP> {
        // 处理正常匹配事件
        @Override
        public void processMatch(Map<String, List<OrderCEP>> match, Context ctx, Collector<String> out) throws Exception {
            OrderCEP payEvent = match.get("pay").get(0);
            out.collect("订单 " + payEvent.orderId + " 已支付！");
        }

        // 处理超时未支付事件
        @Override
        public void processTimedOutMatch(Map<String, List<OrderCEP>> match, Context ctx) throws Exception {
            OrderCEP createEvent = match.get("create").get(0);
            ctx.output(new OutputTag<String>("timeout"){}, "订单 " + createEvent.orderId + " 超时未支付！用户为：" + createEvent.userId);
        }
    }

}
