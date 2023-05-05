package com.lfw.state.ttl;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class StateTtlTest {
    public static void main(String[] args) throws Exception {
        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //开启状态数据的 checkpoint 机制(快照的周期，快照的模式)
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        //开启快照后，就需要指定快照的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        // 开启 task 级别故障自动 failover
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        //需要使用 map 算子来达到一个效果：
        //每来一条数据 (字符串)，输出该条字符串拼接此前到达的所有字符串
        source
                .keyBy(s -> "0")
                .map(new RichMapFunction<String, String>() {

                    //list状态
                    ListState<String> lstState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();

                        //获取一个单值结构的状态存储器，并设置TTL参数
                        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(1000))  //配置数据的存活时长为4s
                                .setTtl(Time.milliseconds(4000))  //配置数据的存活时长为4s

                                .updateTtlOnCreateAndWrite()  //当插入和更新的时候，导致该条数据的ttl计时重置
                                .updateTtlOnReadAndWrite()  //读、写，都导致该条数据的ttl计时重置
                                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)  //设置ttl计时重置的策略

                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 不允许返回已过期但尚未被清理的数据
                                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)  //允许返回已过期但尚未被清理的数据

                                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)  //ttl计时的时间语义:设置为处理时间
                                .useProcessingTime() // ttl计时的时间语义:设置为处理时间

                                // 下面3种过期数据检查清除策略，不是覆盖的关系，而是添加的关系
                                .cleanupIncrementally(1000, true)  // 增量清理（每当一条状态数据被访问，则会检查这条状态数据的ttl是否超时，是就删除）
                                //.cleanupFullSnapshot()  // 全量快照清理策略（在checkpoint的时候，保存到快照文件中的只包含未过期的状态数据，但是它并不会清理算子本地的状态数据）
                                //.cleanupInRocksdbCompactFilter(1000) // 在rockdb的compact机制中添加过期数据过滤器，以在compact过程中清理掉过期状态数据

                                .disableCleanupInBackground()  //禁用后台过期状态的默认清理

                                .build();

                        // 本状态管理器就会执行ttl管理
                        ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("lst", String.class);
                        listStateDescriptor.enableTimeToLive(ttlConfig);
                        lstState = runtimeContext.getListState(listStateDescriptor);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        lstState.add(value);

                        StringBuilder sb = new StringBuilder();
                        for (String s : lstState.get()) {
                            sb.append(s);
                        }

                        return sb.toString();
                    }
                }).setParallelism(1)
                .print().setParallelism(1);

        //提交一个 job
        env.execute();
    }
}
