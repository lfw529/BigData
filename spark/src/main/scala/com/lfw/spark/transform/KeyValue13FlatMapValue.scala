package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KeyValue13FlatMapValue {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("FlatMapValue").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, String)] = sc.makeRDD(Array(("spark", "1, 2, 3"), ("hive", "4,5"), ("hbase", "6"), ("flink", "7,8")))
    //将 value 压平，再将其后的每一个元素与 key 组合
    rdd.flatMapValues(_.split(",").map(_.trim.toInt)).collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
