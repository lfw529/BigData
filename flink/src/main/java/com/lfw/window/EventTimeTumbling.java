package com.lfw.window;

import com.lfw.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;

import java.time.Duration;

public class EventTimeTumbling {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 7777)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        //3.提取数据中的时间戳字段 [老版本]
//        waterSensorDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<WaterSensor>() {
//            @Override
//            public long extractAscendingTimestamp(WaterSensor element) {
//                return element.getTs() * 1000;
//            }
//        });

        //4.1 自增的（延迟为0）
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy1 = WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        //4.2 带有延迟的（含参）
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy2 = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))  //最大容忍延迟时间
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() { //指定时间戳
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        //5.添加策略
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator1 = waterSensorDS.
                assignTimestampsAndWatermarks(waterSensorWatermarkStrategy1);

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator2 = waterSensorDS.
                assignTimestampsAndWatermarks(waterSensorWatermarkStrategy2);  //指定水印和时间戳

        //6.按照id分组
        KeyedStream<WaterSensor, String> keyedStream1 = waterSensorSingleOutputStreamOperator1.keyBy(WaterSensor::getId);

        KeyedStream<WaterSensor, String> keyedStream2 = waterSensorSingleOutputStreamOperator2.keyBy(WaterSensor::getId);

        //7.开窗
        WindowedStream<WaterSensor, String, TimeWindow> window1 = keyedStream1.window(TumblingEventTimeWindows.of(Time.seconds(5)));  //到5触发计算，左闭右开

        WindowedStream<WaterSensor, String, TimeWindow> window2 = keyedStream2.window(TumblingEventTimeWindows.of(Time.seconds(5)));  //到5触发计算，左闭右开

        //8.计算总和
        SingleOutputStreamOperator<WaterSensor> result1 = window1.sum("vc");

        SingleOutputStreamOperator<WaterSensor> result2 = window2.sum("vc");

        //9.打印
        result1.print("自增的");

        result2.print("有延迟的");

        //10.执行任务
        env.execute();
    }
}
