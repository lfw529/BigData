package com.lfw.operator.transform.basic;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RichFilterTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromElements(10, 3, 5, 9, 20, 8)
                .filter(new MyRichFilter()).setParallelism(1)
                .print();
        env.execute();
    }

    public static class MyRichFilter extends RichFilterFunction<Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("aaa");
        }

        @Override
        public boolean filter(Integer value) throws Exception {
            return value % 2 == 0;
        }

        @Override
        public void close() throws Exception {
            System.out.println("bbb");
        }
    }
}
