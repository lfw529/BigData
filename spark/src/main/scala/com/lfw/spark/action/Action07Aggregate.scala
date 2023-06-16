package com.lfw.spark.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Action07Aggregate {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Aggregate").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    //3.1 创建第一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 8)
    //3.2 该 RDD 所有元素相加得到结果
    val result = rdd.aggregate(10)((x, y) => x + y, (x, y) => x + y)
    //val result = rdd.aggregate(10)(_ + _, _ + _) //会调用两次初始值，所以初始值会加2次

    println(result) //100

    //4.关闭连接
    sc.stop()
  }
}
