package com.lfw.operator.sink;

import com.alibaba.fastjson.JSON;
import com.lfw.operator.source.MySourceFunction;
import com.lfw.pojo.EventLog;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.function.SerializableSupplier;

import javax.sql.XADataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 将数据流写入 mysql，利用 JdbcSink 算子
 */
public class SinkToJdbcTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构造好一个数据流
        DataStreamSource<EventLog> streamSource = env.addSource(new MySourceFunction());

        /**
         *  1. 不保证 EOS语义的方式
         */
        SinkFunction<EventLog> jdbcSink = JdbcSink.sink(
                "insert into t_eventlog values (?,?,?,?,?) on duplicate key update session_id=?, event_id=?, ts=?, event_info=? ",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimeStamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        preparedStatement.setString(6, eventLog.getSessionId());
                        preparedStatement.setString(7, eventLog.getEventId());
                        preparedStatement.setLong(8, eventLog.getTimeStamp());
                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://hadoop102:3306/flink_sink?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                        .withUsername("root")
                        .withPassword("1234")
                        .build()
        );

        // 输出数据
//        streamSource.addSink(jdbcSink);

        /**
         * 2. 可以提供 EOS 语义保证的 sink
         */
        SinkFunction<EventLog> exactlyOnceSink = JdbcSink.exactlyOnceSink(
                "insert into t_eventlog values (?,?,?,?,?) on duplicate key update session_id=?, event_id=?, ts=?, event_info=? ",
                new JdbcStatementBuilder<EventLog>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, EventLog eventLog) throws SQLException {
                        preparedStatement.setLong(1, eventLog.getGuid());
                        preparedStatement.setString(2, eventLog.getSessionId());
                        preparedStatement.setString(3, eventLog.getEventId());
                        preparedStatement.setLong(4, eventLog.getTimeStamp());
                        preparedStatement.setString(5, JSON.toJSONString(eventLog.getEventInfo()));

                        preparedStatement.setString(6, eventLog.getSessionId());
                        preparedStatement.setString(7, eventLog.getEventId());
                        preparedStatement.setLong(8, eventLog.getTimeStamp());
                        preparedStatement.setString(9, JSON.toJSONString(eventLog.getEventInfo()));
                    }
                },
                JdbcExecutionOptions.builder()
                        .withMaxRetries(3)
                        .withBatchSize(1)
                        .build(),
                JdbcExactlyOnceOptions.builder()
                        // mysql不支持同一个连接上存在并行的多个事务，必须把该参数设置为true
                        .withTransactionPerConnection(true)
                        .build(),
                new SerializableSupplier<XADataSource>() {
                    @Override
                    public XADataSource get() {
                        // XADataSource就是jdbc连接，不过它是支持分布式事务的连接
                        // 而且它的构造方法，不同的数据库构造方法不同
                        MysqlXADataSource xaDataSource = new MysqlXADataSource();
                        xaDataSource.setUrl("jdbc:mysql://hadoop102:3306/flink_sink?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8");
                        xaDataSource.setUser("root");
                        xaDataSource.setPassword("1234");
                        return xaDataSource;
                    }
                }
        );

        // 输出数据
        streamSource.addSink(exactlyOnceSink);
        env.execute();
    }
}
