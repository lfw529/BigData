package com.lfw.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 1.自定义的Reducer类需要继承Hadoop提供的Reducer类
 * 2.指定4个泛型(2组kv)
 * 输入的kv: 对应mapper中输出的kv
 * key： 单词
 * value: 单词出现了1次
 * <p>
 * 输出的kv:
 * key： 单词  Text
 * value: 每个单词的总次数  IntWritable
 * <p>
 * 3.重写reduce方法.
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //输出的value
    IntWritable outV = new IntWritable();

    /**
     * @param key     某个单词
     * @param values  理解为封装了某个key的所有value
     * @param context Reducer类的上下文对象.负责reducer类的执行.
     *                <p>
     *                一组相同key的kv组会调用一次reduce方法.
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1.累加求和
        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        //封装kv
        outV.set(sum);

        //写出
        context.write(key, outV);
    }
}