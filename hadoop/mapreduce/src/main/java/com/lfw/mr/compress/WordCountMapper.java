package com.lfw.mr.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //输出的key
    Text outK = new Text();
    //输出的value
    IntWritable outV = new IntWritable(1);

    /**
     * @param key     输入的key
     * @param value   输入的value
     * @param context Mapper类的上下文对象.负责mapper类的执行.
     *                <p>
     *                每个输入的kv都会执行一次map方法.
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //切分数据
        String[] words = line.split(" ");
        //将每个单词拼成kv
        for (String word : words) {
            //封装key
            outK.set(word);

            //写出
            context.write(outK, outV);
        }
    }

}
