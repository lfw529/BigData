package com.lfw.mr.writeable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //关联Mapper和Reducer
        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        //设置Map端输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        //设置程序的输入输出路径
        FileInputFormat.setInputPaths(job, new Path("hadoop/mapreduce/input/phone_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hadoop/mapreduce/output/2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
