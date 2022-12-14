package com.lfw.mr.outputformat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

/**
 * 自定义RecordWriter类需要继承RecordWriter
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private String atguiguPath = "hadoop/mapreduce/output/8/atguigu.log";
    private String otherPath = "hadoop/mapreduce/output/8/other.log";
    private PrintWriter atguiguOut;
    private PrintWriter otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        //简单实现:
        /*
        try {
            atguiguOut = new FileOutputStream(new File(atguiguPath));
            otherOut = new FileOutputStream(new File(otherPath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        */

        //获取文件系统对象
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            atguiguOut = new PrintWriter(new OutputStreamWriter(fs.create(new Path(atguiguPath)), StandardCharsets.UTF_8));
            otherOut = new PrintWriter(new OutputStreamWriter(fs.create(new Path(otherPath)), StandardCharsets.UTF_8));
            //fs.open()
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 负责将kv写出到结果文件。
     * <p>
     * 需求: 将包含有"atguigu"的记录写出到 ./atguigu.log,其他的记录写出到./other.log
     *
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        if (line.contains("atguigu")) {
            atguiguOut.println(line);
        } else {
            otherOut.println(line);
        }
    }

    /**
     * 完成一些收尾工作，例如关闭一些资源.
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        atguiguOut.close();
        otherOut.close();
    }
}
