package com.lfw.watermark;

import com.lfw.operator.source.ClickSource;
import com.lfw.pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomPeriodicWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }

    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L;  //延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L;  //观察到的最大时间戳

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            //每来一条数据就调用一次
            System.out.println("取数据中最大的时间戳");
            maxTs = Math.max(event.timestamp, maxTs);  //更新最大时间戳
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //发射水位线，默认200ms调用一次
            System.out.println("生成WaterMark：" + (maxTs - delayTime - 1L));
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}