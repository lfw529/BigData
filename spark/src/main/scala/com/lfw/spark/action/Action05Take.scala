package com.lfw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action05Take {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Take").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 5)
    //3.2 返回RDD中前5个元素
    val takeResult: Array[Int] = rdd.take(5) //备注：取的元素与分区无关，还是拿出数组中的前5个元素
    println(takeResult.mkString(","))

    //4.关闭连接
    sc.stop()
  }
}
