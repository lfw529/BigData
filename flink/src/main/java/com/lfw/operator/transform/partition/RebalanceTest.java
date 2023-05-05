package com.lfw.operator.transform.partition;

import com.lfw.operator.source.ClickSource;
import com.lfw.pojo.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        // 读取数据源，并行度为1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        // 经轮询重分区后打印输出，并行度为4
        stream.rebalance().print("rebalance").setParallelism(4);

        env.execute();
    }
}
