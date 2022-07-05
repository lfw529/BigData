package com.lfw.operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceSocketTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        //3.打印数据
        socketTextStream.print();
        //4.执行任务
        env.execute();
    }
}
