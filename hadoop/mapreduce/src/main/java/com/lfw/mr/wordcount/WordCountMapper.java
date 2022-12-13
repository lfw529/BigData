package com.lfw.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 1.自定义的Mapper类需要继承Hadoop提供的Mapper类.
 * 2.指定4个泛型(2组kv)
 * 输入的kv:
 * key: 偏移量(可以理解为文件中内容读取的位置) LongWritable
 * value: 一行内容  Text
 * <p>
 * 输出的kv:
 * key:  一个单词   Text
 * value:单词出现1次  IntWritable
 * <p>
 * 3. 重写map方法.
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //输出的key
    Text outK = new Text();
    //输出的value
    IntWritable outV = new IntWritable(1);

    /**
     * @param key     输入的key
     * @param value   输入的value
     * @param context Mapper类的上下文对象，负责mapper类的执行
     *                <p>
     *                每个输入的kv都会执行一次map方法
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        //atguigu atguigu
        String line = value.toString();
        //切分数据
        //[atguigu,atguigu]
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
