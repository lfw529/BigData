package com.lfw.mr.mapjoin;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private final Map<String, String> pdMap = new HashMap<>();
    private final Text outK = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //计数器
        context.getCounter("Map Join", "setup").increment(1);

        //加载小表的数据
        //先获取缓存的文件
        URI pdfile = context.getCacheFiles()[0];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(pdfile));

        BufferedReader br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
        String line;
        while ((line = br.readLine()) != null) {
            String[] pds = line.split("\t");
            // 01	小米
            pdMap.put(pds[0], pds[1]);
        }
        //关闭流
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.getCounter("Map Join", "map").increment(1);

        String line = value.toString();
        // 1001	01	1
        String[] split = line.split("\t");
        split[1] = pdMap.get(split[1]);

        String joinResult = split[0] + "\t" + split[1] + "\t" + split[2];
        outK.set(joinResult);

        context.write(outK, NullWritable.get());
    }
}
