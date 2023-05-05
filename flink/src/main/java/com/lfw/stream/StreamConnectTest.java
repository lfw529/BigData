package com.lfw.stream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.io.File;

public class StreamConnectTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        //数字流
        DataStream<Integer> stream1 = env.fromElements(1, 7, 8, 3, 5, 9);
        //字母字符流
        DataStream<String> stream2 = env.fromElements("z", "h", "l", "w", "y");

        /**
         * 流的 connect
         */
        ConnectedStreams<Integer, String> connectedStreams = stream1.connect(stream2);

        SingleOutputStreamOperator<String> resultStream = connectedStreams.map(new CoMapFunction<Integer, String, String>() {
            //共同的状态数据
            final String prefix = "lfw-";

            /**
             * 对 左流 处理的逻辑
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map1(Integer value) throws Exception {
                // 把数字*10，再返回字符串
                return prefix + (value * 10) + "";
            }

            /**
             * 对 右流 处理的逻辑
             * @param value
             * @return
             * @throws Exception
             */
            @Override
            public String map2(String value) throws Exception {
                return prefix + value.toUpperCase();
            }
        });

        resultStream.print();
        env.execute();
    }
}
