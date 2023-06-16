package com.lfw.spark.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CreateRDDFromCollection {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("集合映射RDD").setMaster("local[*]")

    //2.创建SparkContext, 该对象时提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.使用 parallelize() 创建rdd     --方式一
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8))

    rdd.collect().foreach(println)
    println("----------------------")

    //4.使用 makeRDD() 创建 rdd         --方式二，其实makeRDD底层封装了parallelize
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8))

    rdd1.collect().foreach(println)
    sc.stop()
  }
}
