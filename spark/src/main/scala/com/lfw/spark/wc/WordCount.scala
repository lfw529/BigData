package com.lfw.spark.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //TODO 1 创建SparkConf配置文件,并设置应用名称和运行模式
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]") //可以交换顺序
    //TODO 2 构建SparkConf对象，作为编程入口
    val sc = new SparkContext(conf)
    //1 加载数据源
    val lineRDD: RDD[String] = sc.textFile("spark/input/wordcount.txt")
    //2 将一行一行的单词按照空格切割, 拍平 flatMap 算子安排
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
    // 触发执行 (行动算子)
    result.foreach(t => println(t))
    //TODO 3 关闭资源
    sc.stop()
  }
}
