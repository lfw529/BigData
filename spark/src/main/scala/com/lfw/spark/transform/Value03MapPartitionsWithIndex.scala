package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value03MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("MapPartitionsWithIndex").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)

    val indexRdd = rdd.mapPartitionsWithIndex((index, items) => {
      items.map(e => s"partition: $index, val: $e")
    })

    indexRdd.collect().foreach(println)
    //关闭连接
    sc.stop()
  }
}
