package com.lfw.state.backend;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class StateBackend_NEW {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义状态后端，保存状态的位置
        env.setStateBackend(new HashMapStateBackend());
        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        //开启CK
        env.getCheckpointConfig().enableUnalignedCheckpoints();

    }
}
