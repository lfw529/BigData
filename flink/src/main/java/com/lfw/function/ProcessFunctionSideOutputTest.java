package com.lfw.function;

import com.lfw.pojo.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessFunctionSideOutputTest {
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
        //3.使用ProcessFunction将数据分流
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.process(new SplitProcessFunc());

        //4.打印数据
        result.print("主流");
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOut") {
        });
        sideOutput.print("Side");

        //5.执行任务
        env.execute();
    }

    public static class SplitProcessFunc extends ProcessFunction<WaterSensor, WaterSensor> {
        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            //取出水位线
            Integer vc = value.getVc();
            //根据水位线高低，分流
            if (vc >= 30) {
                //将数据输出至主流
                out.collect(value);
            } else {
                //将数据输出至侧输出流
                ctx.output(new OutputTag<Tuple2<String, Integer>>("SideOut") {
                           },
                        new Tuple2<>(value.getId(), vc));
            }
        }
    }
}
