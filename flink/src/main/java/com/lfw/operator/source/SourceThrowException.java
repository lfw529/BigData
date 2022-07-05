package com.lfw.operator.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceThrowException {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new ClickSource()).setParallelism(2).print();

        env.execute();
    }
}
