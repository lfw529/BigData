package com.lfw.operator.sink;

import com.lfw.operator.sink.avro.schema.AvroEventLogBean;
import com.lfw.operator.source.ClickSource;
import com.lfw.pojo.Event;
import com.lfw.pojo.EventLog;
import org.apache.avro.Schema;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetFileFormatFactory;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class SinkToFileForBulkSinkTest_NEW {
    public static void main(String[] args) throws Exception {
        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);
        env.setParallelism(2);

        Map<String, String> map = new HashMap<>();
        map.put("key", "value");

        DataStreamSource<EventLog> stream = env.fromElements(
                new EventLog(1000L, "a1", "QWERTY", 1678255118000L, map),
                new EventLog(1001L, "b1", "ASDFGH", 1678255749000L, map),
                new EventLog(1002L, "c1", "ZXCVBN", 1678255214000L, map),
                new EventLog(1003L, "d1", "AQWSDE", 1678255473000L, map),
                new EventLog(1004L, "e1", "ZASXDC", 1678255335000L, map),
                new EventLog(1005L, "f1", "SFDGXV", 1678255666000L, map)
        );

        String path1 = new File("").getCanonicalPath() + "/flink/output/bulksink/specific";
        String path2 = new File("").getCanonicalPath() + "/flink/output/bulksink/reflect";

        /* 应用 StreamFileSink 算子，来将数据输出到文件系统
         * 列格式：构造一个 FileSink 对象
         *
         * ## 方法1：
         * writerFactory = ParquetAvroWriters.forGenericRecord(schema)
         *
         * ## 方法2：
         * writerFactory = ParquetAvroWriters.forSpecificRecord(AvroEventLogBean.class)
         *
         * ## 方法3：
         * writerFactory = ParquetAvroWriters.forReflectRecord(EventLog.class);
         *
         * 一句话：3种方式需要传入的参数类型不同
         * 1. 需要传入schema对象
         * 2. 需要传入一种特定的JavaBean类class
         * 3. 需要传入一个普通的JavaBean类class
         *
         * 传入这些参数，有何用？
         * 这个工具 ParquetAvroWriters. 需要你提供你的数据模式schema（你要生成的parquet文件中数据模式schema）
         *
         * 上述的3种参数，都能让这个工具明白你所指定的数据模式（schema）
         *
         * 1. 传入Schema类对象，它本身就是parquet框架中用来表达数据模式的内生对象
         *
         * 2. 传入特定JavaBean类class，它就能通过调用传入的类上的特定方法，来获得Schema对象
         * （这种特定JavaBean类，不用开发人员自己去写，而是用avro的schema描述文件+代码生产插件，来自动生成）
         *
         * 3. 传入普通JavaBean,然后工具可以自己通过反射手段来获取用户的普通JavaBean中的包名、类名、字段名、字段类型等信息，来翻译成一个符合Avro要求的Schema
         */


        //方式1：手动构造 schema, 来生成 ParquetAvroWriter 工厂
//        Schema schema = Schema.createRecord("id", "用户id", "com.lfw.User", true);
//        ParquetWriterFactory<GenericRecord> writeFactory0 = ParquetAvroWriters.forGenericRecord(schema);  //根据调用的方法及传入的信息，来获取avro模式schema，并生成对应的parquetWriter

        //2.方式2：利用Avro的规范Bean对象，来生成ParquetAvroWriter工厂
        //需要写 .avsc 文件，并根据文件生成 java 类
        ParquetWriterFactory<AvroEventLogBean> writerFactory1 = AvroParquetWriters.forSpecificRecord(AvroEventLogBean.class);

        FileSink<AvroEventLogBean> parquetSink1 = FileSink
                .forBulkFormat(new Path(path1), writerFactory1)
                .withBucketAssigner(new DateTimeBucketAssigner<AvroEventLogBean>())
                .withBucketCheckInterval(5)
                //bulk模式下的文件滚动策略，只有一种[因为列式文件结构复杂，不好切割]：当checkpoint发生时，进行文件滚动
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("lfw-").withPartSuffix(".parquet").build())
                .build();

        //将上面构造好的sink算子对象，添加到数据流上，进行数据输出

        //报错：eventLogBean.getEventInfo(); 原因：java 不像 scala，不支持协变。即：Student是Person子类，但是 Map<Student>不是 Map<Person>子类
//      stream.map(eventLogBean -> new AvroEventLogBean(eventLogBean.getGuid(), eventLogBean.getSessionId(), eventLogBean.getEventId(), eventLogBean.getTimeStamp(), eventLogBean.getEventInfo()))
        //所以，从上面报错得知，需要作一下转换再处理
        stream.map(eventLogBean -> {
                    HashMap<CharSequence, CharSequence> eventInfo = new HashMap<>();
                    for (Map.Entry<String, String> entry : eventLogBean.getEventInfo().entrySet()) {
                        eventInfo.put(entry.getKey(), entry.getValue());
                    }
                    return new AvroEventLogBean(eventLogBean.getGuid(), eventLogBean.getSessionId(), eventLogBean.getEventId(), eventLogBean.getTimeStamp(), eventInfo);
                }).returns(AvroEventLogBean.class)
                .sinkTo(parquetSink1);

        //方式3：利用Avro的规范Bean对象，来生成ParquetAvroWriter工厂
        //该方法，传入一个普通的JavaBean类，就可以自动通过反射来生成 Schema。
        ParquetWriterFactory<EventLog> writerFactory2 = AvroParquetWriters.forReflectRecord(EventLog.class);

        FileSink<EventLog> parquetSink2 = FileSink
                .forBulkFormat(new Path(path2), writerFactory2)
                .withBucketAssigner(new DateTimeBucketAssigner<EventLog>())
                .withBucketCheckInterval(5)
                //bulk模式下的文件滚动策略，只有一种[因为列式文件结构复杂，不好切割]：当checkpoint发生时，进行文件滚动
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("lfw-").withPartSuffix(".parquet").build())
                .build();

        stream.sinkTo(parquetSink2);

        env.execute();
    }
}
