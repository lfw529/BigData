package com.lfw.window;

import com.lfw.pojo.EventBean;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.*;

/**
 * 全窗口计算 api 使用示例
 * 需求：每隔 10s，统计最近 30s 的数据中，每个用户的行为事件中，行为时长最长的前2条记录
 * 要求用 apply 算子来实现
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
public class WindowProcessTest {
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

        watermarkedBeanStream
                .keyBy(bean -> bean.getPageId())
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
                .process(new ProcessWindowFunction<EventBean, Tuple3<String, String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String key, ProcessWindowFunction<EventBean, Tuple3<String, String, Double>, String, TimeWindow>.Context context, Iterable<EventBean> elements, Collector<Tuple3<String, String, Double>> out) throws Exception {
                        //构造一个 hashmap 来记录每一件事件的发生总次数和行为总时长
                        HashMap<String, Tuple2<Integer, Long>> tmpMap = new HashMap<>();

                        //遍历窗口中的每一条数据
                        for (EventBean element : elements) {
                            String eventId = element.getEventId();
                            Tuple2<Integer, Long> countAndTimeLong = tmpMap.getOrDefault(eventId, Tuple2.of(0, 0L));

                            tmpMap.put(eventId, Tuple2.of(countAndTimeLong.f0 + 1, countAndTimeLong.f1 + element.getActTimelong()));
                        }

                        //然后，从tmpMap中，取到平均时长最大的前两个事件
                        ArrayList<Tuple2<String, Double>> tmpList = new ArrayList<>();
                        for (Map.Entry<String, Tuple2<Integer, Long>> entry : tmpMap.entrySet()) {
                            String eventId = entry.getKey();
                            Tuple2<Integer, Long> tuple = entry.getValue();
                            double avgTimeLong = tuple.f1 / (double) tuple.f0;
                            tmpList.add(Tuple2.of(eventId, avgTimeLong));
                        }

                        //然后对tmpList按平均时长排序
                        Collections.sort(tmpList, new Comparator<Tuple2<String, Double>>() {
                            @Override
                            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                                return Double.compare(o2.f1, o1.f1);
                            }
                        });
                        //输出前2个
                        for (int i = 0; i < Math.min(tmpList.size(), 2); i++) {
                            out.collect(Tuple3.of(key, tmpList.get(i).f0, tmpList.get(i).f1));
                        }
                    }
                }).print();

        env.execute();
    }
}
