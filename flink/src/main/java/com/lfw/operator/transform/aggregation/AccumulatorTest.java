package com.lfw.operator.transform.aggregation;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 测试数据：
 * 1001,sensorreading01,1587778747108,32.3
 * 1001,sensorreading02,1587778756789,36.5
 * 1001,sensorreading01,1587778767891,38.0
 * 1001,sensorreading03,1587778778910,35.4
 * 1001,sensorreading03,1587778789101,33.8
 * 1001,sensorreading01,1587778791011,34.1
 */
public class AccumulatorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool tool = ParameterTool.fromPropertiesFile("flink/src/main/resources/netcat.properties");
        DataStream<String> ds = env.socketTextStream(tool.get("hostname"), tool.getInt("port"));

        ds.map(new RichMapFunction<String, String>() {
            //声明累加器,富函数中声明全局的
            private final LongCounter normal = new LongCounter();
            private final LongCounter exception = new LongCounter();
            private final LongCounter all = new LongCounter();

            //在 open 函数中添加累加器
            public void open(Configuration parameters) throws Exception {
                RuntimeContext rc = getRuntimeContext();
                rc.addAccumulator("normal_temperature", normal);
                rc.addAccumulator("exception_temperature", exception);
                rc.addAccumulator("all_temperature", all);
            }

            @Override
            public String map(String value) throws Exception {
                //进行累加
                all.add(1);
                String msg = "";
                Double temper = new Double(value.split(",")[3]);
                if (temper > 36.4) {
                    //进行累加
                    exception.add(1);
                    msg = value.split(",")[1].trim() + "的体温为：" + temper + ", 体温过高, 需要隔离进行排查！";
                } else {
                    //进行累加
                    normal.add(1);
                    msg = value.split(",")[1].trim() + "的体温为：" + temper + ", 体温正常, 请进入";
                }
                return msg;
            }
        }).print("体温结果为：");

        JobExecutionResult result = env.execute("accumulator");  //关闭无界流之后才会执行后面的部分

        //执行完成后获取累加器的值
        Long normal = result.getAccumulatorResult("normal_temperature");
        Long exception = result.getAccumulatorResult("exception_temperature");
        Long all = result.getAccumulatorResult("all_temperature");

        System.out.println("---------------------------------------------");

        System.out.println("normal: " + normal);
        System.out.println("exception: " + exception);
        System.out.println("all: " + all);
    }
}
