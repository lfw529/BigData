package com.lfw.spark.action

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Action13Sum {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Sum").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 4)
    //sum底层调用的是fold，该方法是一个柯里化方法，第一个括号传入的初始值是0.0
    //第二个括号传入的函数(_ + _)，局部聚合和全局聚合都是相加
    val rdd2: Double = rdd1.sum()
    //在Executor端打印，由于分区数没有限定，所以是无序打印，只有当为1个分区时，有序
    println(rdd2)
    //4.关闭连接
    sc.stop()
  }
}
