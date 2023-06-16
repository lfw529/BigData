package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value08Filter {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Filter").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 2)

    val filterRdd: RDD[Int] = rdd.filter(_ % 2 == 0)
    filterRdd.collect().foreach(println)

    //关闭
    sc.stop()
  }
}
