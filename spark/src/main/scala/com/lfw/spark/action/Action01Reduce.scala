package com.lfw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action01Reduce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Reduce").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //val reduceResult: Int = rdd.reduce((i, j) => i + j)
    val reduceResult: Int = rdd.reduce(_ + _) //备注：行动算子执行后的结果一定不是RDD
    println(reduceResult)

    //4.关闭连接
    sc.stop()
  }
}
