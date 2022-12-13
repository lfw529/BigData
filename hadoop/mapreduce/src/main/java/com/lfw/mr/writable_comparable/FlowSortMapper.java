package com.lfw.mr.writable_comparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private final FlowBean outK = new FlowBean();
    private final Text outV = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String[] flows = line.split("\t");

        //封装key
        outK.setUpFlow(Integer.parseInt(flows[flows.length - 3]));
        outK.setDownFlow(Integer.parseInt(flows[flows.length - 2]));
        outK.setSumFlow();

        //封装value
        outV.set(flows[1]);

        context.write(outK, outV);
    }
}
