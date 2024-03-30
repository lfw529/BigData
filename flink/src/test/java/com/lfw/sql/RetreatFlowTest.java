package com.lfw.sql;

import com.lfw.operator.source.ClickSource;
import com.lfw.pojo.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class RetreatFlowTest {
    public static void main(String[] args) throws Exception {
        //获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据源
        SingleOutputStreamOperator<Event> eventStream = env
                .addSource(new ClickSource());

        //获取表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //将数据流转换成表
        tableEnv.createTemporaryView("EventTable", eventStream);

        //用执行SQL的方式提取数据
        Table urlCountTable = tableEnv.sqlQuery("SELECT user, COUNT(url) as cnt FROM EventTable GROUP BY user");
        tableEnv.toChangelogStream(urlCountTable).print("count");

        // 执行程序
        env.execute();
    }
}
