package com.lfw.tolerance.wal;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Teacher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> s1 = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        s1.transform("x", TypeInformation.of(String.class), new WriteAheadSink(
                new CheckpointCommitter() {
                    @Override
                    public void open() throws Exception {

                    }

                    @Override
                    public void close() throws Exception {

                    }

                    //创建目录
                    @Override
                    public void createResource() throws Exception {
                        Path path = Paths.get("f:/flink/xxx" + this.jobId);
                        if (!Files.exists(path)) {
                            Files.createFile(path);
                        }
                    }

                    //提交 checkpoint 完成
                    @Override
                    public void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception {
                        Path path = Paths.get("f:/flink/xxx" + this.jobId + subtaskIdx);
                        if (!Files.exists(path)) {
                            Files.createFile(path);
                        }
                        System.out.println("提交快照：" + subtaskIdx + "," + checkpointID);
                        Files.write(path, (checkpointID + "").getBytes());
                    }

                    //检查资源给定的检查点是否已完全提交。
                    @Override
                    public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception {
                        Path path = Paths.get("f:/flink/xxx" + this.jobId + subtaskIdx);
                        if (!Files.exists(path)) return false;
                        String idStr = Files.readAllLines(path).get(0);
                        System.out.println(idStr);
                        return checkpointID <= Long.parseLong(idStr);
                    }
                }, TypeInformation.of(String.class).createSerializer(env.getConfig()), "jobx"));

        env.execute();
    }
}
