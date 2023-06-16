package com.lfw.spark.action

import org.apache.spark.{SparkConf, SparkContext}

object Action13MinAndMax {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("MinAndMax").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(5, 7, 9, 6, 1, 8, 2, 4, 3, 10), 4)
    //没有shuffle
    val max: Int = rdd1.max()
    val min: Int = rdd1.min()

    println(max)   //10
    println(min)   //1
    //4.关闭连接
    sc.stop()
  }
}
