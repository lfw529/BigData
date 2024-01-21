package com.lfw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action15Top {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Top").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(
      5, 7, 6, 4,
      9, 6, 1, 7,
      8, 2, 8, 5,
      4, 3, 10, 9
    ), 4)

    var res1: Array[Int] = rdd1.top(2)
    var res2: Array[Int] = rdd1.top(2)(Ordering[Int].reverse)

    res1.foreach(println)  //10 9
    println("------------")
    res2.foreach(println)  //1 2

    sc.stop()
  }
  //指定排序规则，如果没有指定，使用默认的排序规则
  //  implicit val ord: Ordering[Int] = Ordering[Int].reverse
}
