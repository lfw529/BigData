package com.lfw.mr.compress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

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
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //封装kv
        outV.set(sum);

        //写出
        context.write(key, outV);
    }
}
