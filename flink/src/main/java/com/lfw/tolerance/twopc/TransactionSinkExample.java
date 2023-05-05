package com.lfw.tolerance.twopc;

import lombok.SneakyThrows;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;

/**
 * Example program that demonstrates the behavior of a 2-phase-commit (2PC) sink that writes
 * output to files.
 * <p>
 * The 2PC sink guarantees exactly-once output by writing records immediately to a
 * temp file. For each checkpoint, a new temp file is created. When a checkpoint
 * completes, the corresponding temp file is committed by moving it to a target directory.
 * <p>
 * The program includes a MapFunction that throws an exception in regular intervals to simulate
 * application failures.
 * You can compare the behavior of a 2PC sink and the regular print sink in failure cases.
 * <p>
 * - The TransactionalFileSink commits a file to the target directory when a checkpoint completes
 * and prevents duplicated result output.
 * - The regular print() sink writes to the standard output when a result is produced and
 * duplicates result output in case of a failure.
 */
public class TransactionSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //checkpoint every 10 seconds
        env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

        //watermark interval
        env.getConfig().setAutoWatermarkInterval(1000L);

        DataStream<SensorReading> sensorData = env
                //SensorSource生成随机温度读数
                .addSource(new ResettableSensorSource())
                //分配事件时间所需的时间戳和水印
                .assignTimestampsAndWatermarks(new SensorTimeAssigner());

        // 每秒计算所有传感器的平均温度
        SingleOutputStreamOperator<Tuple2<String, Double>> avgTemp = sensorData
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new AllWindowFunction<SensorReading, Tuple2<String, Double>, TimeWindow>() {
                           @Override
                           public void apply(TimeWindow window, Iterable<SensorReading> vals, Collector<Tuple2<String, Double>> out) throws Exception {
                               //定义总温度
                               double sumTemp = 0.0;

                               // 定义平均温度
                               double avgTemp;

                               //从迭代器中取出温度数据放入 arraylist
                               ArrayList<Double> tmpList = new ArrayList<>();

                               //迭代数据, 存入list
                               for (SensorReading s : vals) {
                                   tmpList.add(s.temperature());
                               }

                               for (Double aDouble : tmpList) {
                                   sumTemp = sumTemp + aDouble;
                               }

                               avgTemp = sumTemp / tmpList.size();

                               long epochSeconds = window.getEnd() / 1000;

                               String tString = LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
                                       .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

                               out.collect(Tuple2.of(tString, avgTemp));
                           }
                       }
                )
                //生成触发作业恢复的失败
                .map(new FailingMapper<Tuple2<String, Double>>(16)).setParallelism(1);

        // OPTION 1 (comment out to disable)
        // --------
        // write to files with a transactional sink.
        // results are committed when a checkpoint is completed.
        Tuple2<String, String> tx = createAndGetPaths();

        avgTemp.addSink(new TransactionalFileSink(tx.f0, tx.f1));

        env.execute();
    }

    /**
     * Creates temporary paths for the output of the transactional file sink.
     */
    public static Tuple2<String, String> createAndGetPaths() throws IOException {
        String tempDir = System.getProperty("java.io.tmpdir");
        String targetDir = tempDir + "committed";
        String transactionDir = tempDir + "transaction";

        Path targetPath = Paths.get(targetDir);
        Path transactionPath = Paths.get(transactionDir);

        if (!Files.exists(targetPath)) {
            Files.createDirectory(targetPath);
        }
        if (!Files.exists(transactionPath)) {
            Files.createDirectory(transactionPath);
        }

        return Tuple2.of(targetDir, transactionDir);
    }
}

/**
 * Transactional sink that writes records to files an commits them to a target directory.
 * <p>
 * Records are written as they are received into a temporary file. For each checkpoint, there is
 * a dedicated file that is committed once the checkpoint (or a later checkpoint) completes.
 */
class TransactionalFileSink extends TwoPhaseCommitSinkFunction<Tuple2<String, Double>, String, Void> {

    private final String tempPath;

    private final String targetPath;

    BufferedWriter transactionWriter;

    public TransactionalFileSink(String targetPath, String tempPath) {
        super(TypeInformation.of(String.class).createSerializer(new ExecutionConfig()), TypeInformation.of(Void.class).createSerializer(new ExecutionConfig()));

        this.targetPath = targetPath;
        this.tempPath = tempPath;
    }


    @Override
    protected void invoke(String transaction, Tuple2<String, Double> value, Context context) throws Exception {
        transactionWriter.write(value.toString());
        transactionWriter.write('\n');
    }

    /**
     * Creates a temporary file for a transaction into which the records are written.
     */
    @Override
    protected String beginTransaction() throws Exception {
        String timeNow = LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        int taskIdx = this.getRuntimeContext().getIndexOfThisSubtask();
        String transactionFile = timeNow.substring(0, 10) + "-" + taskIdx;

        // create transaction file and writer
        Path tFilePath = Paths.get(tempPath + transactionFile);
        Files.createFile(tFilePath);
        this.transactionWriter = Files.newBufferedWriter(tFilePath);
        System.out.println("Creating Transaction File: " + tFilePath);

        return transactionFile;
    }

    /**
     * Flush and close the current transaction file.
     */
    @Override
    protected void preCommit(String transaction) throws Exception {
        transactionWriter.flush();
        transactionWriter.close();
    }

    /**
     * Commit a transaction by moving the pre-committed transaction file to the target directory.
     */
    @SneakyThrows
    @Override
    protected void commit(String transaction) {
        Path tFilePath = Paths.get(tempPath + "/" + transaction);
        // check if the file exists to ensure that the commit is idempotent.
        if (Files.exists(tFilePath)) {
            Path cFilePath = Paths.get(targetPath + "/" + transaction);
            Files.move(tFilePath, cFilePath);
        }
    }

    /**
     * Aborts a transaction by deleting the transaction file.
     */
    @SneakyThrows
    @Override
    protected void abort(String transaction) {
        Path tFilePath = Paths.get(targetPath + "/" + transaction);
        if (Files.exists(tFilePath)) {
            Files.delete(tFilePath);
        }
    }
}