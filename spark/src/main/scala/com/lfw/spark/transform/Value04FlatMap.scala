package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value04FlatMap {
  def main(args: Array[String]): Unit = {
    //创建 SparkConf 配置文件, 并设置 App 名称
    val conf = new SparkConf().setAppName("FlatMap").setMaster("local[*]")
    //利用 SparkConf 创建 sc 对象
    val sc = new SparkContext(conf)

    //构建集合数据集
    val arr = Array(
      "spark hive flink",
      "hive hive flink",
      "hive spark flink",
      "hive spark flink"
    )

    val rdd1: RDD[String] = sc.makeRDD(arr, 2)
    val rdd2: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(7)), 2)

    val rdd1ToFlat: RDD[String] = rdd1.flatMap(_.split(" "))
    //打散 +1 操作
    val rdd2ToFlat: RDD[Int] = rdd2.flatMap(_.map(_ + 1))

    rdd1ToFlat.collect().foreach(println)
    println("-----分割线-----")
    rdd2ToFlat.collect().foreach(println)

    sc.stop()
  }
}
