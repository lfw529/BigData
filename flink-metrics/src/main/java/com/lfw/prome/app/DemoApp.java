package com.lfw.prome.app;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

public class DemoApp {
    public static void main(String[] args) throws Exception {
        //0 调试取本地配置，打包部署前要去掉
        Configuration configuration = new Configuration(); // 此行打包部署专用

        // 读取 类加载器路径下的配置 flink-conf.yml
//        String resPath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("")).getPath(); //本地调试专用
//        Configuration configuration = GlobalConfiguration.loadConfiguration(resPath);            //本地调试专用

        // 1. 读取初始化环境
        configuration.setString("metrics.reporter.promgateway.jobName", "demoApp");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        // 2. 指定 nc 的 host 和 port
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 3. 接受socket数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port);

        dataStreamSource.print();

        env.execute("demoApp");
    }
}
