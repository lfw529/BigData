package com.lfw.mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //1. 创建一个Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //2. 关联jar
        job.setJarByClass(WordCountDriver.class);

        //3. 关联mapper 和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4. 设置mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5. 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6. 设置输入和输出路径(本地)
        FileInputFormat.setInputPaths(job, new Path("hadoop/mapreduce/input/hello2.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hadoop/mapreduce/output/6"));

        //设置combiner
        job.setCombinerClass(WordCountCombiner.class);
        //job.setCombinerClass(WordCountReducer.class);//当方法与Reducer一样时

        //7. 提交job
        job.waitForCompletion(true);
    }
}
