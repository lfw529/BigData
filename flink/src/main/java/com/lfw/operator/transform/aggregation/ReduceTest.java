package com.lfw.operator.transform.aggregation;

import com.lfw.operator.source.ClickSource;
import com.lfw.pojo.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                //将 Event 数据类型转换成元组类型
                .map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event e) throws Exception {
                        return Tuple2.of(e.user, 1L);
                    }
                })
                .keyBy(r -> r.f0)  //使用用户名来进行分流
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        //每到一条数据，用户 pv 的统计值加1
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                })   //(Alice,1) (Alice,2) (Alice,3) (Bob,1) (Mary,1) (Bob,2) (Alice,4) (Alice,5) (Bob,3) (Alice,6) (Alice,7) (Bob,4) (Cary,1) (Cary,2) (Cary,3) (Mary,2) (Bob,5) (Bob,6) (Cary,4) (Cary,5) (Mary,3) (Alice,8)  ....
                .keyBy(r -> true) //为每一条数据分配同一个key，将聚合结果发送到一条流中去
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        //将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                })
                .print();//(Alice,1) (Alice,2) (Alice,3) (Alice,4) (Alice,5) (Alice,6) (Alice,7) (Alice,8)  ....
        env.execute();
    }
}
