package com.lfw.operator.transform.basic;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class RichFlatMapFunctionTest {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromElements(1, 2, 3, 4, 5)
                .flatMap(new MyRichFlatMapFunction()).setParallelism(1)
                .print();
        env.execute();
    }

    /**
     * Rich...Function 可以调用 RuntimeContext 类来获取运行时的任务相关信息
     */
    public static class MyRichFlatMapFunction extends RichFlatMapFunction<Integer, Integer> {
        @Override
        public void open(Configuration parameters) throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
            String taskName = runtimeContext.getTaskName();
            System.out.println("aaa " + taskName);
        }

        @Override
        public void flatMap(Integer value, Collector<Integer> out) throws Exception {
            out.collect(value * value);
            out.collect(value * value * value);
        }

        @Override
        public void close() throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
            JobID jobId = runtimeContext.getJobId();
            System.out.println("bbb " + jobId);
        }
    }
}
