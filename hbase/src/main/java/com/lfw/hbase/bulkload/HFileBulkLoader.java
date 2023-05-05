package com.lfw.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;

import java.io.IOException;

/**
 * 导入(驱动)类：负责将 HFileProcessor 生成好的 HFile 文件导入到 HBase 中。
 */
public class HFileBulkLoader {
    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(conf);

        //需提前将表创建好
        TableName tableName = TableName.valueOf("lfw_01:students");

        Admin admin = conn.getAdmin();
        Table table = conn.getTable(tableName);
        //RegionLocator locator = conn.getRegionLocator(tableName);

        //老版本写法
        //LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(conf);
        //loadIncrementalHFiles.doBulkLoad(new Path("hdfs://hadoop102:8020/lfw_students_hfiles/"), admin, table, locator);

        //新版本写法
        BulkLoadHFiles bulkLoadHFiles = new BulkLoadHFilesTool(conf);
        long start = System.currentTimeMillis();
        bulkLoadHFiles.bulkLoad(tableName, new Path("hdfs://hadoop102:8020/lfw_students_hfiles/"));
        long end = System.currentTimeMillis();
        System.out.println("导入总耗时(ms)：" + (end - start));

        table.close();
        admin.close();
        conn.close();
    }
}
