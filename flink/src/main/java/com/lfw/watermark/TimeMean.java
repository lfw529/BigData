package com.lfw.watermark;

import com.lfw.pojo.WaterSensor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.util.Arrays;
import java.util.List;

public class TimeMean {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //使用处理时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        //使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //使用摄取时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_001", 1577844001L, 46),
                new WaterSensor("ws_001", 1577844001L, 47),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));
        //3.从集合读取数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromCollection(waterSensors);
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDataStreamSource.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, GlobalWindow> windowedStream = keyedStream.countWindow(2L);
        SingleOutputStreamOperator<WaterSensor> result = windowedStream.sum("vc");
        result.print();

        env.execute();
    }
}
