package com.lfw.spark.accumulate

import org.apache.spark.{SparkConf, SparkContext}
/**
 * 不使用累加器，而是触发两次Action
 */
object AccumulatorDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo1").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    //对数据进行转换操作（将每个元素乘以10），同时还要统计每个分区的偶数的数量
    val rdd2 = rdd1.map(_ * 10)
    //第一次触发Action
    rdd2.saveAsTextFile("spark/out/AccumulatorDemo1")

    //附加的指标统计
    val rdd3 = rdd1.filter(_ % 2 == 0)
    //第二个触发Action
    val c = rdd3.count()
    println(c)
  }
}
