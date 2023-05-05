package com.lfw.operator.sink;

import com.lfw.operator.source.ClickSource;
import com.lfw.pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;

public class SinkToFileForRowSinkTest_NEW {
    public static void main(String[] args) throws Exception {
        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);
        env.setParallelism(2);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        String path1 = new File("").getCanonicalPath() + "/flink/output/rowsink";

        //应用 StreamFileSink 算子，来将数据输出到文件系统
        //行格式：构造一个 FileSink 对象
        FileSink<String> rowSink = FileSink
                .forRowFormat(new Path(path1), new SimpleStringEncoder<String>("utf-8"))
                //文件的滚动策略 (间隔时长20s, 或文件大下打到1M, 就进行文件切换)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                //至少包含20s的数据
                                .withRolloverInterval(20 * 1000)
                                //最近10s没有接收到新的数据
                                //.withInactivityInterval(TimeUnit.SECONDS.toSeconds(10))
                                //文件大小达到 1M
                                .withMaxPartSize(1024 * 1024)
                                .build())
                //分桶的策略 (划分文件夹的策略)
                .withBucketAssigner(new DateTimeBucketAssigner<String>())
                //每隔5ms检查是否生成新的文件夹来接收数据
                .withBucketCheckInterval(5)
                //输出文件的文件名相关配置
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("lfw-").withPartSuffix(".txt").build())
                .build();

        //然后添加到流，进行输出
        stream.map(Event::toString)
                //.addSink()  /* SinkFunction 实现类对象，用 addSink() 来添加 */
                .sinkTo(rowSink);  /* Sink 的实现类对象，用 sinkTo() 来添加 */

        env.execute();
    }
}
