package com.lfw.window;

import com.lfw.pojo.EventBean;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 迟到数据如何处理的案例
 * <p>
 * 测试数据：
 * 1,e01,10000,p01,10
 * 1,e02,12000,p02,20
 * 1,e02,20000,p03,40
 * 1,e03,16000,p02,10
 * 1,e01,22000,p03,50
 * 1,e04,18000,p04,60
 * 1,e06,19000,p02,10
 */
public class WindowLateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        // 1,e01,3000,pg02
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<EventBean, Integer>> beanStream = source.map(s -> {
                    String[] split = s.split(",");
                    EventBean bean = new EventBean(Long.parseLong(split[0]), split[1], Long.parseLong(split[2]), split[3], Integer.parseInt(split[4]));
                    return Tuple2.of(bean, 1);
                }).returns(new TypeHint<Tuple2<EventBean, Integer>>() {
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<EventBean, Integer>>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<EventBean, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple2<EventBean, Integer> element, long recordTimestamp) {
                                return element.f0.getTimeStamp();
                            }
                        }));

        OutputTag<Tuple2<EventBean, Integer>> lateDataOutputTag = new OutputTag<>("late_data", TypeInformation.of(new TypeHint<Tuple2<EventBean, Integer>>() {
        }));

        SingleOutputStreamOperator<String> sumResult = beanStream.keyBy(tp -> tp.f0.getGuid())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))   //事件时间滚动窗口，窗口长度为10
                .allowedLateness(Time.seconds(2))   //允许迟到2s
                .sideOutputLateData(lateDataOutputTag)  //迟到超过允许时限的数据，输出到该 "outputTag" 所标记的侧流
                .apply(new WindowFunction<Tuple2<EventBean, Integer>, String, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<Tuple2<EventBean, Integer>> input, Collector<String> out) throws Exception {
                        int count = 0;
                        for (Tuple2<EventBean, Integer> eventBeanIntegerTuple2 : input) {
                            count++;
                        }
                        out.collect(window.getStart() + ":" + window.getEnd() + "," + count);
                    }
                });

        DataStream<Tuple2<EventBean, Integer>> lateDataSideStream = sumResult.getSideOutput(lateDataOutputTag);

        sumResult.print("主流结果");

        lateDataSideStream.print("迟到数据");

        env.execute();
    }
}
