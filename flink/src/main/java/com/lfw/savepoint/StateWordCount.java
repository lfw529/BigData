package com.lfw.savepoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateWordCount {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));
        //开启CK, 每 5000ms 开始一次 checkpoint
        env.enableCheckpointing(5000);

        /** 高级选项*/
        //配置精准一次性消费（这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确认 checkpoints 之间的时间会进行 500ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // checkpoint 必须在1分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //允许两个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //使用 externalized checkpoints，这样 checkpoint 在作业取消后仍旧会被保留[Cancel 任务时不删除CK]
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 开启实验性的 unaligned checkpoints
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        //2.读取端口数据并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = env.socketTextStream("hadoop102", 7777)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                });
        //3.按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(data -> data.f0);
        //4.计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //5.打印
        result.print();
        //6.执行任务
        env.execute();
    }
}
