package com.lfw.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HdfsClient {
    /**
     * 三个参数：
     * 1.URI : 指定hdfs的位置，实际上就是 namenode 的地址
     * 2.Configuration : 用于添加配置
     * 3.User : 指定操作 hdfs 的用户
     */
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "lfw");

        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan/"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件上传
     */
    @Test
    public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "lfw");

        // 2 上传文件  [此处须传根路径下的相对目录]
        fs.copyFromLocalFile(new Path("input/sunwukong.txt"), new Path("/xiyou/huaguoshan"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件下载
     */
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "lfw");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("download/sunwukong2.txt"), true);

        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件名更改/移动
     */
    @Test
    public void testRename() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "lfw");

        // 2 修改文件名称
        fs.rename(new Path("/xiyou/huaguoshan/sunwukong.txt"), new Path("/xiyou/huaguoshan/meihouwang.txt"));

        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件夹删除
     */
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "lfw");

        // 2 执行删除
        fs.delete(new Path("/xiyou"), true);

        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件详情查看
     */
    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException {

        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "lfw");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());    //权限
            System.out.println(fileStatus.getOwner());   //拥有者
            System.out.println(fileStatus.getGroup());   //分组
            System.out.println(fileStatus.getLen());     //长度
            System.out.println(fileStatus.getModificationTime());   //修改时间
            System.out.println(fileStatus.getReplication());     //副本数
            System.out.println(fileStatus.getBlockSize());        //块大小
            System.out.println(fileStatus.getPath().getName());    //文件名称

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
        // 3 关闭资源
        fs.close();
    }

    /**
     * 文件和文件夹判断
     */
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "lfw");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:" + fileStatus.getPath().getName());
            } else {
                System.out.println("d:" + fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }
}
