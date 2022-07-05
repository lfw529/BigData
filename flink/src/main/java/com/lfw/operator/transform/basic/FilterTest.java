package com.lfw.operator.transform.basic;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.fromElements(10, 3, 5, 9, 20, 8)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value % 2 == 0;
                    }
                }).print();
        env1.execute();

        System.out.println("----------------------lambda 表达式方式--------------------------");

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.fromElements(10, 3, 5, 9, 20, 8)
                .filter(value -> value % 2 == 0)
                .print();
        env2.execute();
    }
}
