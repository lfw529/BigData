package com.lfw.spark.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDDFromFile {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.1 读取文件。如果是本地路径：D:\IdeaProjects\bigdata0821\SparkCoreTest0821\input\2.txt   --测试1
    val lineWordRdd1: RDD[String] = sc.textFile("spark/input/data_01.txt")
    //3.2 读取文件。如果是集群路径：hdfs://hadoop102:8020/input   --测试2
    val lineWordRdd2: RDD[String] = sc.textFile("hdfs://hadoop102:8020/spark/input")

    //4.打印
    lineWordRdd1.foreach(println)
    println("-------------------------")
    lineWordRdd2.foreach(println)

    //5.关闭
    sc.stop()
  }
}
