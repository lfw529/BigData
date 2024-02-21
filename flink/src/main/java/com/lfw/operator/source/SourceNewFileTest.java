package com.lfw.operator.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class SourceNewFileTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String path = new File("").getCanonicalPath() + "/flink/input/clicks.csv";

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(path)).build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(),"file")
                .print();

        env.execute();
    }

}
