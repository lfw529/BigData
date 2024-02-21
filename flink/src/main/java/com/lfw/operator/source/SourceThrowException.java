package com.lfw.operator.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceThrowException {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为2
        env.addSource(new ClickSource()).setParallelism(2).print();

        env.execute();
    }
}
