package com.lfw.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestCompress {
    /**
     * 压缩：使用支持压缩的输出流将数据写入到文件中
     */
    @Test
    public void testCompress() throws IOException, ClassNotFoundException {
        //待压缩文件
        String srcFile = "input/shediaoyingxiong.txt";
        //压缩后文件
        String destFile = "output/sdyx.txt";

        //输入流
        FileInputStream fis = new FileInputStream(new File(srcFile));

        //输出流
        //使用的压缩格式
        String compressClassName = "org.apache.hadoop.io.compress.DefaultCodec";
        Class<?> compressClass = Class.forName(compressClassName);

        //编解码器对象
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(compressClass, conf);

        FileOutputStream fos = new FileOutputStream(new File(destFile + codec.getDefaultExtension()));

        CompressionOutputStream codecOut = codec.createOutputStream(fos);

        //读写数据
        IOUtils.copyBytes(fis, codecOut, conf);

        //关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(codecOut);
    }

    /**
     * 解压缩：使用支持解压缩的流从压缩文件中读取数据
     */
    @Test
    public void testDeCompress() throws IOException {
        //待压缩文件
        String srcFile = "output/sdyx.txt.deflate";
        //压缩后文件
        String destFile = "output/shediaoyingxiong.txt";

        //输入流
        FileInputStream fis = new FileInputStream(new File(srcFile));
        //编解码器对象
        Configuration conf = new Configuration();
        CompressionCodec codec = new CompressionCodecFactory(conf).getCodec(new Path(srcFile));
        CompressionInputStream codecIn = codec.createInputStream(fis);

        //输出流
        FileOutputStream fos = new FileOutputStream(new File(destFile));

        //读写数据
        IOUtils.copyBytes(codecIn, fos, conf);
        //关闭流
        IOUtils.closeStream(codecIn);
        IOUtils.closeStream(fos);
    }
}
