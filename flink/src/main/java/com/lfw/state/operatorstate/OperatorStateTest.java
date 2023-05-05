package com.lfw.state.operatorstate;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class OperatorStateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        // 开启状态数据的checkpoint机制（快照的周期，快照的模式）
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        // 开启快照后，就需要指定快照数据的持久化存储位置
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        // 开启 task 级别故障自动 failover
        // env.setRestartStrategy(RestartStrategies.noRestart()); // 默认是，不会自动failover；一个task故障了，整个job就失败了
        // 使用的重启策略是：固定重启上限和重启时间间隔
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);

        // 需要使用 map 算子来达到一个效果：
        // 每来一条数据 (字符串)，输出该条字符串拼接此前到达过的所有字符串
        source.map(new StateMapFunction()).print();

        // 提交一个job
        env.execute();
    }
}

/**
 * 要使用 operator state，需要让用户自己的 Function 类去实现 CheckpointedFunction
 * 然后在其中的 initializeState 方法中，去拿到 operator state 存储器
 */
class StateMapFunction implements MapFunction<String, String>, CheckpointedFunction {

    ListState<String> listState;

    /**
     * 正常的MapFunction的处理逻辑方法
     *
     * @param value The input value.
     * @return
     * @throws Exception
     */
    @Override
    public String map(String value) throws Exception {
        /* 测试点：故意埋一个异常，来测试 task 级别自动容错效果 */  //结合上面的重启策略，超过3次，就不会重启，job 挂掉
        if (value.equals("x") && RandomUtils.nextInt(1, 15) % 4 == 0)
            throw new Exception("哈哈哈哈,出错了");

        // 将本条数据，插入到状态存储器中
        listState.add(value);

        //然后拼接历史以来的字符串
        Iterable<String> strings = listState.get();
        StringBuilder sb = new StringBuilder();
        for (String string : strings) {
            sb.append(string);
        }
        return sb.toString();
    }

    /**
     * 系统对状态数据做快照（持久化）时会调用的方法，用户利用这个方法，在持久化前，对状态数据做一些操控
     *
     * @param context the context for drawing a snapshot of the operator
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
//        System.out.println("checkpoint 触发了，checkpointId: " + context.getCheckpointId());
    }

    /**
     * 算子任务在启动之初，会调用下面的方法，来为用户进行状态数据初始化
     *
     * @param context the context for initializing the operator
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //从方法提供的context中拿到一个算子状态存储器
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();

        //算子状态存储器，只提供List数据结构来为用户存储数据
        ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<String>("string", String.class); // 定义一个状态存储结构描述器

        // getListState方法，在task失败后，task自动重启时，会帮用户自动加载最近一次的快照状态数据
        // 如果是job重启，则不会自动加载此前的快照状态数据
        listState = operatorStateStore.getListState(stateDescriptor);  //在状态存储器上调用 get 方法，得到具体结构的状态管理器

        /**
         * unionListState 和普通 ListState 的区别：
         * unionListState 的快照存储数据，在系统重启后，list 数据的重分配模式为广播模式；在每个 subtask 上都拥有一份完整的数据
         * ListState 的快照存储数据，在系统重启后，list 数据的重分配模式为 round-robin；轮询平均分配
         */
        //ListState<String> unionListState = operatorStateStore.getUnionListState(stateDescriptor);

    }
}