package com.lfw.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置App名称和运行模式
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]") //可以交换顺序
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //1 利用sc获取文件,一行一行读取单词
    val lineRDD: RDD[String] = sc.textFile("spark/input/wordcount.txt")
    //2 将一行一行的单词按照空格切割,拍平  flatMap算子安排
    //    val wordRDD: RDD[String] = lineRDD.flatMap(line=>line.split(" "))   完整版
    val wordRDD: RDD[String] = lineRDD.flatMap(_.split(" "))
    //3.将一个个的单词转换成(单词,1) map 算子安排
    //    val word2oneRDD: RDD(String,Int)] = wordRDD.map(word =>(word,1))
    val word2oneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    //4 按照key分组 然后聚合进行规约操作 reduceByKey算子安排
    //    val word2sumRDD: RDD[(String, Int)] = word2oneRDD.reduceByKey((v1, v2) => (v1 + v2))
    val word2sumRDD: RDD[(String, Int)] = word2oneRDD.reduceByKey(_ + _)
    //将统计结果采集到控制台打印
    val result: Array[(String, Int)] = word2sumRDD.collect()
    result.foreach(t => println(t))
    //TODO 3 关闭资源
    sc.stop()
  }
}
