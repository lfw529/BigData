package com.lfw.operator.sink;

import com.lfw.pojo.Event;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class SinkToFileTest_OLD {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> stream1 = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        DataStreamSource<Event> stream2 = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        DataStreamSource<Event> stream3 = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        DataStreamSource<Event> stream4 = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L));

        String path1 = new File("").getCanonicalPath() + "/flink/output/writeAsText";
        String path2 = new File("").getCanonicalPath() + "/flink/output/writeAsCsv";
        String path3 = new File("").getCanonicalPath() + "/flink/output/writeUsingOutputFormat";
        //textfile
        stream1.map(bean -> Tuple3.of(bean.user, bean.url, bean.timestamp)).returns(new TypeHint<Tuple3<String, String, Long>>() {
                })
                .keyBy(bean -> bean.f0)
                .sum("f2")
                .writeAsText(path1, FileSystem.WriteMode.OVERWRITE);
        //csv
        stream2.map(bean -> Tuple3.of(bean.user, bean.url, bean.timestamp)).returns(new TypeHint<Tuple3<String, String, Long>>() {
                })
                .keyBy(bean -> bean.f0)
                .sum("f2")
                .writeAsCsv(path2, FileSystem.WriteMode.OVERWRITE);
        //OutputFormat
        stream3.map(bean -> Tuple3.of(bean.user, bean.url, bean.timestamp)).returns(new TypeHint<Tuple3<String, String, Long>>() {
                })
                .keyBy(bean -> bean.f0)
                .sum("f2")
                .writeUsingOutputFormat(new TextOutputFormat<>(new Path(path3)));

        //写到socket
        stream4.map(bean -> Tuple3.of(bean.user, bean.url, bean.timestamp)).returns(new TypeHint<Tuple3<String, String, Long>>() {
                })
                .keyBy(bean -> bean.f0)
                .sum("f2")
                .writeToSocket("hadoop102", 9999, new SerializationSchema<Tuple3<String, String, Long>>() {
                    @Override
                    public byte[] serialize(Tuple3<String, String, Long> element) {
                        return new byte[0];
                    }
                });
        env.execute();
    }
}
