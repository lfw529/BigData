package com.lfw.operator.transform.aggregation;

import com.lfw.pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PojoAggregationTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Mary", "./cart", 4000L)
        );
        // --max--> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
        // --max--> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
        // --max--> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:04.0}
        stream.keyBy(e -> e.user).max("timestamp").print("--max--");  //  局部替换最大值


        // --maxBy--> Event{user='Mary', url='./home', timestamp=1970-01-01 08:00:01.0}
        // --maxBy--> Event{user='Bob', url='./cart', timestamp=1970-01-01 08:00:02.0}
        // --maxBy--> Event{user='Mary', url='./cart', timestamp=1970-01-01 08:00:04.0}
        stream.keyBy(e -> e.user).maxBy("timestamp").print("--maxBy--");    // 根据key最大值,整体对象替换

        env.execute();
    }
}
