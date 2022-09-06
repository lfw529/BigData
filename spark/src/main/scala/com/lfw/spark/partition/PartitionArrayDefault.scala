package com.lfw.spark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionArrayDefault {
  def main(args: Array[String]): Unit = {

    //1.创建 SparkConf 并设置 App 名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建 SparkContext,该对象是提交 Spark App 的入口
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))

    //3.输出数据，产生了8个分区
    rdd.saveAsTextFile("spark/output")

    //结论：利用集合创建RDD,如果不手动指定分区数，默认是spark程序用的总核心数，当然也可以通过setMaster("local[4]")来设置核心个数
    //4.关闭连接
    sc.stop()
  }
}
