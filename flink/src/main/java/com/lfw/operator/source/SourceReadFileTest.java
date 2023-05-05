package com.lfw.operator.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.io.File;

public class SourceReadFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = new File("").getCanonicalPath() + "/flink/input/clicks.csv";

        DataStreamSource<String> lines = env.readFile(new TextInputFormat(null), path);

        //PROCESS_CONTINUOUSLY 模式是一直监听指定的文件或目录，2 秒钟检测一次文件是否发生变化;
        //文件的内容发生变化后，会将以前的内容和新的内容全部都读取出来，进而造成数据重复读取。一般不使用
//        DataStreamSource<String> lines2 = env.readFile(new TextInputFormat(null), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000);

        lines.print();

        env.execute();
    }
}
