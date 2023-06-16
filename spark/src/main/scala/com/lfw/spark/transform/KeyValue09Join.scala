package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KeyValue09Join {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Join").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //通过并行化的方式创建一个RDD
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)), 2)
    //通过并行化的方式再创建一个RDD
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2), ("jerry", 4)), 2)

    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
    rdd3.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
