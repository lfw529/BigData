package com.lfw.mr.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private final OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1.获取一行
        String line = value.toString();
        //10000001	Pdt_01	222.8

        //2.读取
        String[] fields = line.split("\t");

        //3.封装对象
        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));

        //4.写出
        context.write(k, NullWritable.get());  //hadoop 的 key 不同于 java 中的 map,可以重复，且 value
        //是一个对象
    }
}
