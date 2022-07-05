package com.lfw.operator.transform.aggregation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class IterateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        DataStream<Integer> initialStream = env.fromElements(10, -3);

        //创建一个IterativeStream
        IterativeStream<Integer> iteration = initialStream.iterate();

        //在IterativeStream执行map转换
        //SingleOutputStreamOperator 表示用户定义的转换应用于具有一种预定义输出类型的 {@link DataStream}。 @param <T> 此流中元素的类型。
        SingleOutputStreamOperator<Integer> iterationBody = iteration.map(new MyRichMapFunction());
        //大于0的值进入反馈循环通道，小于0的值发送给下游
        SingleOutputStreamOperator<Integer> feedback = iterationBody.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value > 0;
            }
        });
        iteration.closeWith(feedback);
        SingleOutputStreamOperator<Integer> output = iterationBody.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value <= 0;
            }
        });
        //打印输出
        iterationBody.print();
        env.execute();
    }

    //对每个元素每次减3
    private static class MyRichMapFunction extends RichMapFunction<Integer, Integer> {
        // 默认生命周期方法, 初始化方法, 在每个并行度上只会被调用一次
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open ... 执行一次");
        }

        @Override
        public Integer map(Integer value) throws Exception {
            System.out.println("map ... 一个元素执行一次");
            return value - 3;
        }

        // 默认生命周期方法, 最后一个方法, 做一些清理工作, 在每个并行度上只调用一次
        @Override
        public void close() throws Exception {
            System.out.println("close ... 执行一次");
        }
    }
}
