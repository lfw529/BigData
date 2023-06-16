package com.lfw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action04First {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("First").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //返回RDD中元素的个数
    val firstResult: Int = rdd.first()
    println(firstResult)

    //4.关闭连接
    sc.stop()
  }
}
