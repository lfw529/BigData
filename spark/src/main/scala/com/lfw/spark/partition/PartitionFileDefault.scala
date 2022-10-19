package com.lfw.spark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionFileDefault {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //默认分区的数量
    val rdd: RDD[String] = sc.textFile("spark/input/data_01.txt")

    rdd.saveAsTextFile("spark/output3")
    //结论：利用外部文件创建rdd，分区数默认为min(总核心数，2) 一般都是两个分区
    //关闭
    sc.stop()
  }
}
