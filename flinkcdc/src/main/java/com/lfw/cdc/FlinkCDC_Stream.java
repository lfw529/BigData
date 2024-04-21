package com.lfw.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC_Stream {
    public static void main(String[] args) throws Exception {
        //1. 获取 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 开启 checkpoint [测试可以注释掉]
//        //Flink-CDC 将读取 binlog 的位置信息以状态方式保存在CK,如果想要做到断点续传，需要从Checkpoint或者Savepoint启动程序
//        //开启Checkpoint，每隔5s钟做一次CK
//        env.enableCheckpointing(5000L);
//        //指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //设置任务关闭的时候保留最后一次 CK 数据
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //指定从 CK 自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //设置状态后端
//        env.setStateBackend(new HashMapStateBackend()).getCheckpointConfig().
//                setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop102:8020/flink/checkpoint"));
//        //设置访问 HDFS 的用户名
//        System.setProperty("HADOOP_USER_NAME", "lfw");

        //3. 使用 flinkcdc 构建 mysqlSource
//        StartupOptions
//        initial (default): Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
//         [首次启动时对监控的数据库表进行初始快照，并继续读取最新的binlog。]
//
//        latest-offset: Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
//         [只需从 binlog 的末尾读取，这意味着只有自连接器启动以来的更改。]
//
//        timestamp: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp. The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
//         [从不在第一次启动时对被监控的数据库表进行快照，直接从指定的时间戳读取binlog。消费者将从头开始遍历binlog，忽略时间戳小于指定时间戳的变更事件。]
//
//        specific-offset: Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified
//         [首次启动时从不对被监控的数据库表进行快照，直接从指定位置读取binlog]

        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("1234")
                .databaseList("flinkcdc")
                .tableList("flinkcdc.t1") //可选配置项，如果不指定该参数，则会读取上一个配置下所有表的数据，注意：指定的时候需要使用"db.table"的方式
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())  //官方提供的序列化
                .build();

        //4. 读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");
        //5. 打印
        mysqlDS.print();
        //6. 启动
        env.execute();
    }
}
