package com.lfw.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountCluster {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称和运行模式
    val conf = new SparkConf().setAppName("WordCount")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1 利用sc获取文件，一行一行读取单词
    val lineRDD: RDD[String] = sc.textFile(args(0)) //只传一个参数
    //2.将一行一行的单词按照空格切割，拍平 flatMap 算子安排
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    //3.将一个个的单词 转换成(单词,1) map算子安排
    val word2oneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    //4.按照key分组 然后聚合进行规约操作 reduceByKey 算子安排
    val word2sumRDD: RDD[(String, Int)] = word2oneRDD.reduceByKey(_ + _)
    val result: Array[(String, Int)] = word2sumRDD.collect()
    result.foreach(println)
    //关闭资源
    sc.stop()
  }
}
