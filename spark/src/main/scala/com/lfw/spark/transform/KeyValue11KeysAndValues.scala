package com.lfw.spark.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object KeyValue11KeysAndValues {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("KeysValues").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val list = List(("spark", 1), ("hadoop", 1), ("hive", 1), ("spark", 1), ("flink", 1), ("hbase", 1), ("hive", 1))

    //通过并行化的方式创建 RDD，分区数量为4
    val wordAndOne: RDD[(String, Int)] = sc.parallelize(list, 4)
    val keysRDD: RDD[String] = wordAndOne.keys
    val valuesRDD: RDD[Int] = wordAndOne.values

    keysRDD.collect().foreach(println)
    println("----------分割线-------------")
    valuesRDD.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }
}
