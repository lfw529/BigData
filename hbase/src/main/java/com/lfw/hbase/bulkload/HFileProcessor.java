package com.lfw.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * HFile 文件生成类
 * 本 mapreduce，需要保证输出的 kv，要严格全局有序。
 * 案例：将 studnets.txt 数据集转换成 HFile 文件并存在 hdfs 指定目录上
 */
public class HFileProcessor {

    //TODO 1: 创建 HFileProcessorMapper 类继承 Mapper 类，泛型为：
    // a. 输入key: LongWritable
    // b. 输入value: Text
    // c. 输出key: ImmutableBytesWritable
    // d. 输出value: KeyValue
    public static class HFileProcessorMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        //map() 函数获取到文本行
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context context) throws IOException, InterruptedException {
            //000046,GxrgmE,33,female,北京,26505,2017-01-14
            String[] split = value.toString().split(",");
            //对 rowkey 进行 MD5 加密
            String rkMd5 = MD5Hash.getMD5AsHex(Bytes.toBytes(split[0]));

            //将这一行原始数据拆分成了6个KeyValue输出
            //rowkey:base_info:年龄
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rkMd5)), new KeyValue(Bytes.toBytes(rkMd5), Bytes.toBytes("base_info"), Bytes.toBytes("age"), Bytes.toBytes(Integer.parseInt(split[2]))));
            //rowkey:base_info:性别
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rkMd5)), new KeyValue(Bytes.toBytes(rkMd5), Bytes.toBytes("base_info"), Bytes.toBytes("gender"), Bytes.toBytes(split[3])));
            //rowkey:base_info:姓名
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rkMd5)), new KeyValue(Bytes.toBytes(rkMd5), Bytes.toBytes("base_info"), Bytes.toBytes("name"), Bytes.toBytes(split[1])));
            //rowkey:extra_info:城市
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rkMd5)), new KeyValue(Bytes.toBytes(rkMd5), Bytes.toBytes("extra_info"), Bytes.toBytes("city"), Bytes.toBytes(split[4])));
            //rowkey:extra_info:毕业时间
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rkMd5)), new KeyValue(Bytes.toBytes(rkMd5), Bytes.toBytes("extra_info"), Bytes.toBytes("graduate"), Bytes.toBytes(split[6])));
            //rowkey:extra_info:薪水
            context.write(new ImmutableBytesWritable(Bytes.toBytes(rkMd5)), new KeyValue(Bytes.toBytes(rkMd5), Bytes.toBytes("extra_info"), Bytes.toBytes("salary"), Bytes.toBytes(Integer.parseInt(split[5]))));
        }
    }

    public static void main(String[] args) throws Exception {
        //加载配置文件
        Configuration conf = HBaseConfiguration.create();
        //配置JOB
        Job job = Job.getInstance(conf);
        //需要加载的类
        job.setJarByClass(HFileProcessor.class);
        job.setMapperClass(HFileProcessorMapper.class);

        //配置输出格式
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);  //设置映射输出数据的键类
        job.setMapOutputValueClass(KeyValue.class);  //设置映射输出数据的值类

        job.setOutputKeyClass(ImmutableBytesWritable.class);  //设置作业输出数据的键类。
        job.setOutputValueClass(KeyValue.class);  //设置作业输出的值类

        //设置 reducer 的个数
        job.setNumReduceTasks(0);

        //对HFileOutputFormat2类，做一些配置
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("lfw_01:students");
        Table table = conn.getTable(tableName);
        //获取表的 Region 检索对象
        RegionLocator regionLocator = conn.getRegionLocator(tableName);
        //配置MapReduce作业以执行对给定表的增量加载。
        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

        job.setOutputFormatClass(HFileOutputFormat2.class);

        //待写入数据集读取路径
        FileInputFormat.setInputPaths(job, new Path("hbase/data/students.txt"));
        //写入数据集目标路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop102:8020/lfw_students_hfiles/"));

        //提交 job
        job.waitForCompletion(true);
    }
}
