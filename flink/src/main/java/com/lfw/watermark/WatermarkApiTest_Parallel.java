package com.lfw.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//测试算子不同并行度下的watermark传递情况是否一致？ 每个并行度下传递的watermark不一致。但最终会取 Min(各个并行度中的 watermarks)

/**
 * 测试数据：
 * 1,e01,1577844005000,pg01
 * 2,e02,1577844003000,pg01
 * 3,e03,1577844002000,pg01
 * 4,e04,1577844008000,pg01
 * 5,e05,1577844007000,pg01
 */
public class WatermarkApiTest_Parallel {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(1000);

        // 1,e01,168673487846,pg01
        DataStreamSource<String> s1 = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<EventBean> s2 = s1.map(s -> {
                    String[] split = s.split(",");
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                }).returns(EventBean.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<EventBean>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                                    @Override
                                    public long extractTimestamp(EventBean eventBean, long recordTimestamp) {
                                        return eventBean.getTimeStamp();
                                    }
                                })

                ).setParallelism(2);   //这是并行度为2

        s2.process(new ProcessFunction<EventBean, Object>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, Object>.Context ctx, Collector<Object> out) throws Exception {
                Thread.sleep(10000);
                System.out.println("睡醒了，准备打印");

                //打印此刻的 watermark
                long processTime = ctx.timerService().currentProcessingTime();
                long watermark = ctx.timerService().currentWatermark();

                System.out.println("本次收到的数据: " + eventBean);
                System.out.println("此刻的watermark: " + watermark);
                System.out.println("此刻的处理时间(processing time): " + processTime);

                out.collect(eventBean);
            }
        }).setParallelism(2).print();

        env.execute();
    }
}
