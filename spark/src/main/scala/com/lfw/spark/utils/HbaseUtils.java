package com.lfw.spark.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * 简易版：仅供测试使用
 */
public class HbaseUtils {
    public static Connection getConnection() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop102:2181,hadoop103:2181,hadoop104:2181");
        // 创建一个 hbase 客户端连接
        return ConnectionFactory.createConnection(conf);
    }
}
