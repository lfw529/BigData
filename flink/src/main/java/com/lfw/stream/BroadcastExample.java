package com.lfw.stream;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class BroadcastExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //模拟数据源
        DataStreamSource<String> filterData = env.addSource(new RichSourceFunction<String>() {
            private boolean isRunning = true;
            String[] data = new String[]{"java", "python", "scala"};

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = data.length;
                while (isRunning) {
                    //每分钟一条
                    //TimeUnit.MINUTES.sleep(1);
                    TimeUnit.SECONDS.sleep(3);
                    int seed = (int) (Math.random() * size);
                    ctx.collect(data[seed]);
                    System.out.println("第一个流发送了:" + data[seed]);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        //打印测试
        //filterData.print();
        //定义数据广播规则
        MapStateDescriptor<String, String> configFilter = new MapStateDescriptor<String, String>("configFilter", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        //对数据进行广播
        BroadcastStream<String> broadcastConfig = filterData.setParallelism(1).broadcast(configFilter);

        //模拟第二个数据源
        DataStreamSource<String> dataStream = env.addSource(new RichSourceFunction<String>() {
            private boolean isRunning = true;
            //测试数据集
            final String[] data = new String[]{
                    "java代码量太大",
                    "python代码量少，易学习",
                    "php是web开发语言",
                    "scala流式处理语言，主要应用于大数据开发场景",
                    "go是一种静态强类型、编译型、并发型，并具有垃圾回收功能的编程语言"
            };

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int size = data.length;
                while (isRunning) {
                    //TimeUnit.SECONDS.sleep(3);
                    TimeUnit.SECONDS.sleep(1);
                    int seed = (int) (Math.random() * size);
                    ctx.collect(data[seed]);
                    System.out.println("第二个流发送了:" + data[seed]);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        //对广播数据进行关联
        DataStream<String> resultStream = dataStream.connect(broadcastConfig).process(new BroadcastProcessFunction<String, String, String>() {
            //设置拦截关键字
            private String keyWords = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                keyWords = "java";
                System.out.println("初始化keyWords：" + keyWords);
            }

            //每条数据都会处理
            @Override
            public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                if (value.contains(keyWords)) {
                    out.collect("拦截消息:" + value + ", 原因: 包含拦截关键字：" + keyWords);
                }
            }

            //对广播变量获取更新
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                keyWords = value;
                System.out.println("更新关键字：" + value);
            }
        });

        resultStream.print("result");
        env.execute("BroadcastExample");
    }
}
