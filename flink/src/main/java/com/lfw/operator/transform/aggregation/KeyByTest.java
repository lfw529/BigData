package com.lfw.operator.transform.aggregation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 == 0 ? "偶数" : "奇数";
                    }
                }).print();
        env1.execute();

        System.out.println("----------------------lambda 表达式方式--------------------------");

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(value -> value % 2 == 0 ? "偶数" : "奇数")
                .print();
        env2.execute();
    }
}

