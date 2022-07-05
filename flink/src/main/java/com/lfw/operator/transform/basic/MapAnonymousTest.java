package com.lfw.operator.transform.basic;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapAnonymousTest {
    public static void main(String[] args) throws Exception {
        //匿名内部类
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.fromElements(1, 2, 3, 4, 5)
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }
                }).print();
        env1.execute();

        System.out.println("----------------------lambda 表达式方式--------------------------");

        StreamExecutionEnvironment env2 = StreamExecutionEnvironment.getExecutionEnvironment();
        env2.fromElements(1, 2, 3, 4, 5)
                .map(ele -> ele * ele).print();

        env2.execute();
    }
}
