package com.lfw.spark.serializable

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import java.net.InetAddress

object SerTest02 {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    //从 HDFS 中读取数据，创建 RDD
    //HDFS 指定的目录中有4个小文件，内容如下：
    //1. 1,ln
    val lines = sc.textFile("spark/input/SerTest/")

    //函数外部定义的一个引用类型（变量）
    //RuleObjNotSer是一个类，需要new才能实现（实在Driver被初始化的）
    val rulesObj = RuleObjectSer

    //处理数据，关联维度
    val func = (line: String) => {
      val fields = line.split(",")
      val id = fields(0).toInt
      val code = fields(1)
      val name = rulesObj.rulesMap.getOrElse(code, "未知") //闭包
      //获取当前线程ID
      val treadId = Thread.currentThread().getId
      //获取当前Task对应的分区编号
      val partitionId = TaskContext.getPartitionId()
      //获取当前Task运行时的所在机器的主机名
      val host = InetAddress.getLocalHost.getHostName
      (id, code, name, treadId, partitionId, host, rulesObj.toString)
    }

    //处理数据，关联维度
    val res = lines.map(func)
    res.foreach(t => println(t))
    sc.stop()
  }
}
