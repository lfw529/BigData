package com.lfw.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();

        //设置HDFS NameNode的地址
        configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");
        // 指定MapReduce运行在Yarn上
        configuration.set("mapreduce.framework.name", "yarn");
        // 指定mapreduce可以在远程集群运行
        configuration.set("mapreduce.app-submission.cross-platform", "true");
        //指定Yarn ResourceManager的位置
        configuration.set("yarn.resourcemanager.hostname", "hadoop103");

        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJar("hadoop/mapreduce/target/mapreduce-1.0-SNAPSHOT.jar");

        // 3 设置map和reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0])); //从下面的图片中的方框出传入参数
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
