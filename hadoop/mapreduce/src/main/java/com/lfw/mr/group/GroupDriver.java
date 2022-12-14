package com.lfw.mr.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GroupDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"hadoop/mapreduce/input/GroupingComparator.txt",
                "hadoop/mapreduce/output/7"};
        //1.获取配置信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2.配置Jar加载路径
        job.setJarByClass(GroupDriver.class);

        //3.加载map/reduce
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);

        //4.设置map输出数据key和value类型 (输入数据被泛型写死，所以不用，查看InputFormat抽象类)
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        //5.设置最终输出数据的key和value类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //6.设置输入数据和输出数据路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.设置分组比较器
        job.setGroupingComparatorClass(GroupComparator.class);
        job.waitForCompletion(true);
    }
}

