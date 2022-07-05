package com.lfw.operator.transform.aggregation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class CoMapFunctionTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取端口数据创建流
        DataStreamSource<String> socketTextStream1 = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> socketTextStream2 = env.socketTextStream("hadoop102", 9999);

        //将 socketTextStream2 转换为 Int 类型
        SingleOutputStreamOperator<Integer> intDS = socketTextStream2.map(String::length);
        //连接两个流
        ConnectedStreams<String, Integer> connectedStreams = socketTextStream1.connect(intDS);
        //处理连接之后的流
        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {  //输入字符串
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {  //计数
                return value;
            }
        });
        result.print();

        env.execute();
    }
}
