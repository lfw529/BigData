package com.lfw.operator.sink;

import com.alibaba.fastjson.JSON;
import com.lfw.operator.source.MySourceFunction;
import com.lfw.pojo.EventLog;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Optional;

/**
 * 将数据流写入redis，利用RedisSink算子
 */
public class SinkToRedisTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        // 创建一个到redis连接的配置
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("192.168.6.102").setPort(6379).build();

        RedisSink<EventLog> redisSink = new RedisSink<>(config, new StringInsertMapper());

        streamSource.addSink(redisSink);

        env.execute();
    }

    /**
     * SET 结构数据插入
     */
    static class StringInsertMapper implements RedisMapper<EventLog> {
        //说明使用 redis 的数据结构类型
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        /**
         * 如果选择的是没有内部key的redis数据结构，则此方法返回的就是大 key
         * 如果选择的是有内部key的redis数据结构(hset)，则此方法返回的是hset内部的小key，二把上面Description中传入的值作为大key
         *
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(EventLog data) {
            return data.getGuid() + "-" + data.getSessionId() + "-" + data.getTimeStamp();  //这里就是string数据的大Key
        }

        @Override
        public String getValueFromData(EventLog data) {
            return JSON.toJSONString(data);   //这里就是string数据的value
        }
    }

    /**
     * HASH 结构数据插入
     */
    static class HSetInsertMapper implements RedisMapper<EventLog> {
        //可以根据具体数据，选择额外key (就是hash这种结构，他有额外key (大 key))
        @Override
        public Optional<String> getAdditionalKey(EventLog data) {
            return RedisMapper.super.getAdditionalKey(data);
        }

        // 可以根据具体数据，设置不同的TTL (time to live，数据的存活时长)
        @Override
        public Optional<Integer> getAdditionalTTL(EventLog data) {
            return RedisMapper.super.getAdditionalTTL(data);
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "event-logs");
        }

        /**
         * 如果选择的是没有内部key的redis数据结构，则此方法返回的就是大 key
         * 如果选择的是有内部key的redis数据结构 (hset)，则此方法返回的是hset内部的小key，二把上面Description中传入的值作为大key
         *
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(EventLog data) {
            return data.getGuid() + "-" + data.getSessionId() + "-" + data.getTimeStamp();  // 这里就是hset中的field (小 key))
        }

        @Override
        public String getValueFromData(EventLog data) {
            return data.getEventId();   // 这里就是hset中的value
        }
    }
}
