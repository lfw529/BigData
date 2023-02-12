package com.lfw.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //设置HDFS NameNode的地址
        configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
        // 指定MapReduce运行在Yarn上
        configuration.set("mapreduce.framework.name", "yarn");
        // 指定mapreduce可以在远程集群运行
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        //指定Yarn ResourceManager的位置
        configuration.set("yarn.resourcemanager.hostname", "hadoop103");
        //指定提交队列
        configuration.set("mapreduce.job.queuename", "prod"); //--------------提交

        //1. 获取一个Job实例
        Job job = Job.getInstance(configuration);

        //1.1 设置jar加载路径 [仓库路径]
        job.setJar("hadoop/yarn/target/yarn-1.0-SNAPSHOT.jar");  //不设置路径回报错，找不到类

        //2. 设置类路径
        job.setJarByClass(WcDriver.class);

        //3. 设置Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4. 设置Mapper和Reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setCombinerClass(WordCountReducer.class);

        //5. 设置输入输出文件  args[0] = hdfs://hadoop102:8020/input_yarn  args[1] = hdfs://hadoop102:8020/output_yarn
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6. 提交Job
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
