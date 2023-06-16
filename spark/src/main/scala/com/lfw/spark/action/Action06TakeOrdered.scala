package com.lfw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action06TakeOrdered {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("TakeOrdered").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(7, 1, 2, 10, 3, 9, 5, 6, 4, 8))
    //3.2 返回RDD中排完序后的前5个元素
    val arr1: Array[Int] = rdd.takeOrdered(5)
    println(arr1.mkString(","))
    //3.3 如何返回6,7,8,9,10
    val arr2: Array[Int] = rdd.takeOrdered(5)(Ordering[Int].reverse).reverse
    println(arr2.mkString(","))

    //4.关闭连接
    sc.stop()
  }
}
