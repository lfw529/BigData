package com.lfw.operator.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class SourceFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = new File("").getCanonicalPath() + "/flink/input/clicks.csv";
        DataStream<String> stream = env.readTextFile(path);
        stream.print();

        env.execute();
    }
}
