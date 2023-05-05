package com.lfw.operator.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import com.lfw.operator.source.MySourceFunction;
import com.lfw.pojo.EventLog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Desc: 要把处理好的数据流，输出到文件系统（hdfs）
 * 使用的sink算子，是扩展包中的 StreamFileSink
 **/
public class BulkSinkMethod3 {
    public static void main(String[] args) throws Exception {
        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 将上面的数据流输出到文件系统（假装成一个经过了各种复杂计算后的结果数据流）
        String path1 = new File("").getCanonicalPath() + "/flink/output/bulksink/method3";

        /**
         * 方式三：
         * 核心逻辑：
         *   - 利用自己的JavaBean类，来构造一个 parquetWriterFactory
         *   - 利用parquetWriterFactory构造一个FileSink算子
         *   - 将原始数据流，输出到 FileSink算子
         */

        // 2. 通过自己的JavaBean类，来得到一个parquetWriter
        ParquetWriterFactory<EventLog> parquetWriterFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);

        // 3. 利用生成好的parquetWriter，来构造一个 支持列式输出parquet文件的 sink算子
        FileSink<EventLog> bulkSink = FileSink.forBulkFormat(new Path(path1), parquetWriterFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<EventLog>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("lfw-").withPartSuffix(".parquet").build())
                .build();

        // 5. 输出数据
        streamSource.sinkTo(bulkSink);

        env.execute();
    }
}
