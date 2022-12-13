package com.lfw.mr.writable_partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        //1  13736230513  192.168.100.1   www.atguigu.com  2481  24681  200
        String line = value.toString();
        //切割
        String[] flows = line.split("\t");
        //封装key
        outK.set(flows[1]);
        //封装value
        outV.setUpFlow(Integer.parseInt(flows[flows.length - 3]));
        outV.setDownFlow(Integer.parseInt(flows[flows.length - 2]));
        outV.setSumFlow();

        //写出
        context.write(outK, outV);
    }
}
