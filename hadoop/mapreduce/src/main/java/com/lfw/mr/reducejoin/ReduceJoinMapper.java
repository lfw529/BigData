package com.lfw.mr.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, OrderPdBean> {

    private final Text outK = new Text();
    private final OrderPdBean outV = new OrderPdBean();

    private FileSplit currentSplit;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取切片对象
        currentSplit = (FileSplit) context.getInputSplit();
        System.out.println("setup:" + currentSplit);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] orderpds = line.split("\t");

        if (currentSplit.getPath().toString().contains("order")) {
            //来自于order的数据
            //1001	01	1
            outK.set(orderpds[1]);
            outV.setOrderId(orderpds[0]);
            outV.setPid(orderpds[1]);
            outV.setAmount(Integer.parseInt(orderpds[2]));
            outV.setPname("");
            outV.setTitle("order");
        } else {
            //来自于pd的数据
            //01	小米
            outK.set(orderpds[0]);
            outV.setPid(orderpds[0]);
            outV.setPname(orderpds[1]);
            outV.setOrderId("");
            outV.setAmount(0);
            outV.setTitle("pd");
        }
        //写出
        context.write(outK, outV);
    }
}
