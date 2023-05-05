package com.lfw.stream;

import com.alibaba.fastjson.JSON;
import com.lfw.operator.source.MySourceFunction;
import com.lfw.pojo.EventLog;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.File;

/**
 * 测流输出 代码示例 (process算子)
 */
public class SideOutputTest {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);

        String pathCk = new File("").getCanonicalPath() + "/flink/output/checkpoint";
        // 开启checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///" + pathCk);

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        //需求：将行为事件流，进行分流
        //     appLaunch 事件，分到一个流
        //     putBack 事件，分到一个流
        //     其他事件保留在主流
        SingleOutputStreamOperator<EventLog> processed = streamSource.process(new ProcessFunction<EventLog, EventLog>() {
            /**
             * @param eventLog  输入数据
             * @param ctx 上下文，它能提供“测输出“功能
             * @param out 主流输出收集器
             */
            @Override
            public void processElement(EventLog eventLog, ProcessFunction<EventLog, EventLog>.Context ctx, Collector<EventLog> out) throws Exception {
                String eventId = eventLog.getEventId();

                if ("appLaunch".equals(eventId)) {
                    //定义侧输出流的输出标签
                    ctx.output(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)), eventLog);
                } else if ("putBack".equals(eventId)) {
                    //定义侧输出流的输出标签
                    ctx.output(new OutputTag<String>("back", TypeInformation.of(String.class)), JSON.toJSONString(eventLog));
                }
                //主流输出(还是包含测流数据)，只是不需要打印
                out.collect(eventLog);
            }
        });

        //获取launch侧流数据
        DataStream<EventLog> launchStream = processed.getSideOutput(new OutputTag<EventLog>("launch", TypeInformation.of(EventLog.class)));

        //获取back侧流数据
        DataStream<String> backStream = processed.getSideOutput(new OutputTag<String>("back", TypeInformation.of(String.class)));

        launchStream.print("launch");

        backStream.print("back");

        env.execute();
    }
}
