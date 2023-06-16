package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value05Glom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Glom").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val rdd2: RDD[Array[Int]] = rdd.glom()

    val array: Array[Array[Int]] = rdd2.collect()
    //遍历
    for (elem <- array) {
      println(elem.mkString(","))
    }
    println("---------------------------")
    val maxRDD: RDD[Int] = rdd2.map(arr => arr.max)
    maxRDD.collect().foreach(println)
    //取数组中最大值求和
    println(maxRDD.sum())

    sc.stop()
  }
}