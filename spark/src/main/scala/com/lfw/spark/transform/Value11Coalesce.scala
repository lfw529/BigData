package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value11Coalesce {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Coalesce").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.创建一个 RDD
    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    println("------------原始数据----------------")
    rdd1.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    }).collect().foreach(println)
    println("------------分界线----------------")
    //3.1 缩减分区:不执行 shuffle
    val coalesceRdd = rdd1.coalesce(2)
    //4.创建一个 RDD
    var rdd2: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    //4.1 缩减分区:执行 shuffle
    val coalesceRDD = rdd2.coalesce(2, true)
    //5 查看对应分区数据
    val indexRdd = coalesceRdd.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    val indexRDD = coalesceRDD.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    //带分区打印数据
    indexRdd.collect().foreach(println)
    println("------------分界线----------------")
    indexRDD.collect().foreach(println)
    //延迟一段时间，观察http://localhost:4040页面，查看Shuffle读写数据
    Thread.sleep(Long.MaxValue)
    //关闭
    sc.stop()
  }
}
