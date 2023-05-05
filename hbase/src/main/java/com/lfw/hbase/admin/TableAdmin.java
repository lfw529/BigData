package com.lfw.hbase.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * DDL: DATA DEFINE LANGUAGE
 * 建表，修改表定义，删表, ...
 */
public class TableAdmin {

    private Connection connection;
    private Admin admin;

    //TODO: 首先连接 hbase 的客户端
    @BeforeTest
    public void beforeTest() throws IOException {
        // 1.create方法会自动去加载运行时的classpath中的hbase-site.xml等配置文件
        Configuration conf = HBaseConfiguration.create();
        //由于拷贝了hbase-site.xml到resources，所以不用再去zookeeper中取IP信息
        //conf.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");

        // 2.创建一个Hbase连接
        connection = ConnectionFactory.createConnection(conf);
        // 3.拿到一个DDL的操作工具
        // 要创建表、删除表需要和HMaster连接，所以需要有一个admin对象
        admin = connection.getAdmin();
    }

    /**
     * 创建名称空间
     */
    @Test
    public void createNamespaceTest() throws IOException {
        NamespaceDescriptor descriptor = NamespaceDescriptor.create("lfw_01").build();
        admin.createNamespace(descriptor);
    }

    /**
     * TODO: 创建表
     */
    @Test
    public void createTableTest() throws IOException {
        //创建一个表名对象
        TableName tableName = TableName.valueOf("lfw_01:teachers");
        //1.判断表是否存在
        admin.tableExists(tableName);//存在，则退出

        // 2.构建表描述构建器
        // TableDescriptor: 表描述器，描述这个表有几个列蔟、其他的属性都是在这里可以配置
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);

        // 3.构建列蔟描述构建器
        // 经常会使用到一个工具类：Bytes(hbase包下的Bytes工具类)，这个工具类可以将字符串、long、double类型转换成byte[]数组，也可以将byte[]数组转换为指定类型
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("f1".getBytes());
        //通过列蔟描述构造器设置列族参数
        columnFamilyDescriptorBuilder.setMaxVersions(2);

        //4.构建列蔟描述对象
        ColumnFamilyDescriptor familyDescriptor = columnFamilyDescriptorBuilder.build();

        //建立表描述构造器，设置列族
        tableDescriptorBuilder.setColumnFamily(familyDescriptor);

        //创建一个表描述对象
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();

        //5.用admin来创建这个表
        admin.createTable(tableDescriptor);
    }

    @Test
    public void deleteTableTest() throws IOException {
        TableName tableName = TableName.valueOf("lfw_01:teachers");
        // 1.判断表是否存在
        if (admin.tableExists(tableName)) {
            // 2.如果存在，则禁用表
            admin.disableTable(tableName);
            // 3.再删除表
            admin.deleteTable(tableName);
        }
    }

    @AfterTest
    public void afterTest() throws IOException {
        // 4.使用admin.close、connection.close关闭连接
        admin.close();
        connection.close();
    }
}
