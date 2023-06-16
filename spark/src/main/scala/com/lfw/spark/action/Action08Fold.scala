package com.lfw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action08Fold {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Fold").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 5) //5个分区
    //3.2 将该RDD所有元素相加得到结果
    val foldResult: Int = rdd.fold(10)(_ + _) //会调用两次初始值，所以初始值会加2次
    println(foldResult)

    //4.关闭连接
    sc.stop()
  }
}
