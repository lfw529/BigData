package com.lfw.operator.transform.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TupleAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 3),
                Tuple2.of("b", 3),
                Tuple2.of("b", 4)
        );

        stream.keyBy(r -> r.f0).sum(1).print("---1---");  //---1---> (a,1)  ---1---> (a,4)  ---1---> (b,3)  ---1---> (b,7)
        stream.keyBy(r -> r.f0).sum("f1").print("---2---");  //---2---> (a,1)  ---2---> (a,4)  ---2---> (b,3)  ---2---> (b,7)
        stream.keyBy(r -> r.f0).max(1).print("---3---");  //---3---> (a,1)  ---3---> (a,3)  ---3---> (b,3)  ---3---> (b,4)
        stream.keyBy(r -> r.f0).max("f1").print("---4---");  //---4---> (a,1)  ---4---> (a,3)  ---4---> (b,3)  ---4---> (b,4)
        stream.keyBy(r -> r.f0).min(1).print("---5---");  //---5---> (a,1)  ---5---> (a,1)  ---5---> (b,3)  ---5---> (b,3)
        stream.keyBy(r -> r.f0).min("f1").print("---6---");  //---6---> (a,1)  ---6---> (a,1)  ---6---> (b,3)  ---6---> (b,3)
        stream.keyBy(r -> r.f0).maxBy(1).print("---7---");  //---7---> (a,1)  ---7---> (a,3)  ---7---> (b,3)   ---7---> (b,4)
        stream.keyBy(r -> r.f0).maxBy("f1").print("---8---");  //---8---> (a,1)  ---8---> (a,3)  ---8---> (b,3)  ---8---> (b,4)
        stream.keyBy(r -> r.f0).minBy(1).print("---9---");  //---9---> (a,1)  ---9---> (a,1)  ---9---> (b,3)  ---9---> (b,3)
        stream.keyBy(r -> r.f0).minBy("f1").print("---10---"); //---10---> (a,1)  ---10---> (a,1)  ---10---> (b,3)  ---10---> (b,3)

        env.execute();
    }
}
