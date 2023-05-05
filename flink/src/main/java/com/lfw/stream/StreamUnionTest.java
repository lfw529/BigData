package com.lfw.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class StreamUnionTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        //数字流1
        DataStream<Integer> stream1 = env.fromElements(1, 7, 8);
        //数字流2
        DataStream<Integer> stream2 = env.fromElements(3, 5, 9);

        //合并两条流
        DataStream<Integer> resultStream = stream1.union(stream2);
        resultStream.print();

        env.execute();
    }
}

