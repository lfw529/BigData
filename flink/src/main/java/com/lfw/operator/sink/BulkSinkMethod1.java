package com.lfw.operator.sink;

import com.lfw.operator.source.MySourceFunction;
import com.lfw.pojo.EventLog;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.core.fs.Path;

import java.io.File;

/**
 * 如果出现报错 org.apache.parquet.io.OutputFile.getPath()Ljava/lang/String;，
 * 这是由于 parquet-avro 版本与 flink-parquet 中的版本不一致导致的依赖冲突
 *  flink1.16  -->  parquet-avro 1.12.2
 *  flink1.14  -->  parquet-avro 1.11.1
 */
public class BulkSinkMethod1 {
    public static void main(String[] args) throws Exception {
        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 将上面的数据流输出到文件系统（假装成一个经过了各种复杂计算后的结果数据流）

        /**
         * 方式一：
         * 核心逻辑：
         *   - 构造一个schema
         *   - 利用schema构造一个parquetWriterFactory
         *   - 利用parquetWriterFactory构造一个FileSink算子
         *   - 将原始数据转成GenericRecord流，输出到FileSink算子
         */
        // 1. 先定义GenericRecord的数据模式
        Schema schema = SchemaBuilder.builder()
                .record("DataRecord")
                .namespace("com.lfw.sink.avro.schema")
                .doc("用户行为事件数据模式")
                .fields()
                .requiredInt("gid")
                .requiredLong("ts")
                .requiredString("eventId")
                .requiredString("sessionId")
                .name("eventInfo")
                .type()
                .map()
                .values()
                .type("string")
                .noDefault()
                .endRecord();

        String path1 = new File("").getCanonicalPath() + "/flink/output/bulksink/method1";

        // 2. 通过定义好的schema模式，来得到一个parquetWriter
        ParquetWriterFactory<GenericRecord> writerFactory = AvroParquetWriters.forGenericRecord(schema);

        // 3. 利用生成好的parquetWriter，来构造一个 支持列式输出parquet文件的 sink算子
        FileSink<GenericRecord> sink1 = FileSink.forBulkFormat(new Path(path1), writerFactory)
                .withBucketAssigner(new DateTimeBucketAssigner<GenericRecord>("yyyy-MM-dd--HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("lfw-").withPartSuffix(".parquet").build())
                .build();

        // 4. 将自定义javabean的流，转成上述sink算子中parquetWriter所需要的GenericRecord流
        SingleOutputStreamOperator<GenericRecord> recordStream = streamSource
                .map((MapFunction<EventLog, GenericRecord>) eventLog -> {
                    // 构造一个Record对象
                    GenericData.Record record = new GenericData.Record(schema);

                    // 将数据填入record
                    record.put("gid", (int) eventLog.getGuid());
                    record.put("eventId", eventLog.getEventId());
                    record.put("ts", eventLog.getTimeStamp());
                    record.put("sessionId", eventLog.getSessionId());
                    record.put("eventInfo", eventLog.getEventInfo());

                    return record;
                }).returns(new GenericRecordAvroTypeInfo(schema));  // 由于avro的相关类、对象需要用avro的序列化器，所以需要显式指定AvroTypeInfo来提供AvroSerializer

        // 5. 输出数据
        recordStream.sinkTo(sink1);

        env.execute();
    }
}
