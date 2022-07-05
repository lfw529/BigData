package com.lfw.operator.transform.basic;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RichFlatMapFunctionTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromElements(1, 2, 3, 4, 5)
                .flatMap(new MyRichFlatMapFunction()).setParallelism(1)
                .print();
        env.execute();
    }

    public static class MyRichFlatMapFunction extends RichFlatMapFunction<Integer, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("aaa");
        }

        @Override
        public void flatMap(Integer value, Collector<Integer> out) throws Exception {
            out.collect(value * value);
            out.collect(value * value * value);
        }

        @Override
        public void close() throws Exception {
            System.out.println("bbb");
        }
    }
}
