package com.lfw.window;

import com.lfw.pojo.EventBean;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * 全窗口计算 api 使用示例
 * 需求：每隔10s，统计最近 30s 的数据中，每个页面上发生的行为中，平均时长最大的前2种事件及其平均时长
 * 要求用 process 算子来实现
 * <p>
 * 测试数据：
 * 1,e01,10000,p01,10
 * 1,e02,11000,p02,20
 * 1,e02,12000,p03,40
 * 1,e03,20000,p02,10
 * 1,e01,21000,p03,50
 * 1,e04,22000,p04,10
 * 1,e06,28000,p05,60
 * 1,e07,30000,p02,10
 */
public class WindowApplyTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<EventBean> beanStream = source.map(s -> {
            String[] split = s.split(",");
            return new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
        }).returns(EventBean.class);

        // 分配 watermark，以推进事件时间
        SingleOutputStreamOperator<EventBean> watermarkedBeanStream = beanStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                            @Override
                            public long extractTimestamp(EventBean eventBean, long recordTimestamp) {
                                return eventBean.getTimeStamp();
                            }
                        })
        );

        SingleOutputStreamOperator<EventBean> resultStream = watermarkedBeanStream.keyBy(EventBean::getGuid)
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                //泛型1: 输入数据类型；泛型2: 输出结果类型 泛型3: key的类型；泛型4: 窗口类型
                .apply(new WindowFunction<EventBean, EventBean, Long, TimeWindow>() {
                    /**
                     * @param key 本次传给咱们的窗口是属于哪个key的
                     * @param window 本次传给咱们的窗口的各种元信息（比如本窗口的起始时间，结束时间）
                     * @param input 本次传给咱们的窗口中所有数据的迭代器
                     * @param out 结果数据输出器
                     * @throws Exception
                     */
                    @Override
                    public void apply(Long key, TimeWindow window, Iterable<EventBean> input, Collector<EventBean> out) throws Exception {
                        //从迭代器中迭代出数据，放入一个 arraylist，然后排序，输出前2条
                        ArrayList<EventBean> tmpList = new ArrayList<>();

                        //迭代数据，存入list
                        for (EventBean eventBean : input) {
                            tmpList.add(eventBean);
                        }

                        //排序
                        Collections.sort(tmpList, new Comparator<EventBean>() {
                            @Override
                            public int compare(EventBean o1, EventBean o2) {
                                return o2.getActTimelong() - o1.getActTimelong();
                            }
                        });

                        //输出前2条
                        for (int i = 0; i < Math.min(tmpList.size(), 2); i++) {
                            out.collect(tmpList.get(i));
                        }
                    }
                });
        resultStream.print();

        env.execute();
    }
}
