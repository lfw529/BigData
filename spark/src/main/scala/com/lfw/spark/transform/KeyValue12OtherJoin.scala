package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KeyValue12OtherJoin {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("OtherJoin").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    //通过并行化的方式再创建一个RDD
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7), ("tom", 3)))

    //leftOuterJoin
    val rdd3: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    //rightOuterJoin
    val rdd4: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)

    //fullOuterJoin
    val rdd5: RDD[(String, (Option[Int], Option[Int]))] = rdd1.fullOuterJoin(rdd2)

    rdd3.collect().foreach(println)
    println("---------分割线-----------")
    rdd4.collect().foreach(println)
    println("---------分割线-----------")
    rdd5.collect().foreach(println)
    //4.关闭连接
    sc.stop()
  }
}
