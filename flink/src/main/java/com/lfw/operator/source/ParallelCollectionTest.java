package com.lfw.operator.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

public class ParallelCollectionTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);  //默认并行度

        DataStreamSource<Integer> fromElements = env.fromElements(1, 2, 3, 4, 5);
        fromElements.map(d -> d * 10)/*.print()*/;

        //fromParallelCollection所返回的算子，是一个并行度的source算子
        DataStreamSource<LongValue> parallelCollection = env.fromParallelCollection(new LongValueSequenceIterator(1, 100), TypeInformation.of(LongValue.class));
        parallelCollection.map(lv -> lv.getValue() + 100).print();

        DataStreamSource<Long> sequence = env.generateSequence(1, 100);
        sequence.map(x -> x - 1).print();

        env.execute();
    }
}
