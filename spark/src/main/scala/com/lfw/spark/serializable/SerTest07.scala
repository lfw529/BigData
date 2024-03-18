package com.lfw.spark.serializable

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import java.net.InetAddress

object SerTest07 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //从 HDFS 中读取数据，创建 RDD
    //HDFS 指定的目录中有4个小文件，内容如下：
    //1. 1,ln
    val lines = sc.textFile("spark/input/SerTest/")

    //处理数据，关联维度
    val res = lines.mapPartitions(it => {
      //RuleClassNotSer是在Executor中被初始化的
      //一个分区的多条数据，使用同一个RuleClassNotSer实例
      val rulesClass = new RuleClassNotSer
      it.map(e => {
        val fields = e.split(",")
        val id = fields(0).toInt
        val code = fields(1)
        //但是如果每来一条数据 new 一个 RuleClassNotSer，不好，效率低，浪费资源，频繁GC
        val name = rulesClass.rulesMap.getOrElse(code, "未知")
        //获取当前线程ID
        val treadId = Thread.currentThread().getId
        //获取当前Task对应的分区编号
        val partitionId = TaskContext.getPartitionId()
        //获取当前Task运行时的所在机器的主机名
        val host = InetAddress.getLocalHost.getHostName
        (id, code, name, treadId, partitionId, host, rulesClass.toString)
      })
    })
    res.foreach(t => println(t))
    sc.stop()
  }
}
