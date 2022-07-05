package com.lfw.operator.transform.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.fromElements(1, 2, 3, 4, 5)
                .flatMap(new FlatMapFunction<Integer, Integer>() {
                    @Override
                    public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                        out.collect(value * value);
                        out.collect(value * value * value);
                    }
                }).print();
        env1.execute();

        System.out.println("----------------------lambda 表达式方式--------------------------");

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.fromElements(1, 2, 3, 4, 5)
                .flatMap((Integer value, Collector<Integer> out) -> {
                    out.collect(value * value);
                    out.collect(value * value * value);
                }).returns(Types.INT).print();
        env2.execute();
    }
}
