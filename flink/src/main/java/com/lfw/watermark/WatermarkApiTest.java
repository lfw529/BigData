package com.lfw.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

//测试 watermark 的传递和数据的传递是同步的还是异步的？
public class WatermarkApiTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);

        // 1,e01,168673487846,pg01
        SingleOutputStreamOperator<String> s1 = env.socketTextStream("hadoop102", 9999).disableChaining();

        // 策略1： WatermarkStrategy.noWatermarks()  不生成 watermark，禁用了事件时间的推进机制
        // 策略2： WatermarkStrategy.forMonotonousTimestamps()  紧跟最大事件时间
        // 策略3： WatermarkStrategy.forBoundedOutOfOrderness()  允许乱序的 watermark生成策略
        // 策略4： WatermarkStrategy.forGenerator()  自定义watermark生成算法

        /** 示例 1：从最源头算子开始，生成 watermark */
        // 1.构造一个watermark的生成策略对象 (算法策略，及事件时间的抽取方法)
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMillis(0))    //允许乱序的算法策略
                .withTimestampAssigner((element, recordTimestamp) -> Long.parseLong(element.split(",")[2]));  //时间戳抽取方法
        //2.将构造好的 watermark 策略对象，分配给流 (source 算子)
        /*s1.assignTimestampsAndWatermarks(watermarkStrategy).print();*/

        /**
         * 示例 2：不从最源头算子开始生成 watermark，而是从中间环节的某个算子开始生成 watermark
         * 注意：如果在源头就已经生成了 watermark，就不要在下游再次产生 watermark
         */
        SingleOutputStreamOperator<EventBean> s2 = s1.map(s -> {
                    String[] split = s.split(",");
                    Thread.sleep(10000);
                    System.out.println("睡醒了，准备打印");
                    return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3]);
                }).returns(EventBean.class).disableChaining()
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<EventBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                            @Override
                            public long extractTimestamp(EventBean eventBean, long recordTimestamp) {
                                return eventBean.getTimeStamp();
                            }
                        })
                );
        s2.process(new ProcessFunction<EventBean, EventBean>() {
            @Override
            public void processElement(EventBean eventBean, ProcessFunction<EventBean, EventBean>.Context ctx, Collector<EventBean> out) throws Exception {
                out.collect(eventBean);
            }
        }).startNewChain().print();
        env.execute();
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class EventBean {
    private long guid;
    private String eventId;
    private long timeStamp;
    private String pageId;
}
