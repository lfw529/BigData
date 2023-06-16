package com.lfw.spark.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Value13SortBy {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SortBy").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("spark/input/wordcount.txt")
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和1组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    //分组聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //按照单词出现的次数，从高到低进行排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    //5 打印
    sorted.collect().foreach(println)
    //6.关闭
    sc.stop()
  }
}
