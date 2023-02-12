package com.lfw.mr.compress;

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
        //1.创建一个Job对象
        Configuration conf = new Configuration();

        //开启map输出的压缩
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec");

        //开启reduce输出的压缩
        conf.set("mapreduce.output.fileoutputformat.compress", "true");
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.DefaultCodec");

        Job job = Job.getInstance(conf);
        //2.关联Jar
        job.setJarByClass(WordCountDriver.class);

        //3.关联mapper/reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4. 设置mapper输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //5. 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //6. 设置输入和输出路径(本地)
        FileInputFormat.setInputPaths(job, new Path("hadoop/mapreduce/input/shediaoyingxiong.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hadoop/mapreduce/output/13/sdyx"));

        //7. 提交job
        job.waitForCompletion(true);
    }
}
