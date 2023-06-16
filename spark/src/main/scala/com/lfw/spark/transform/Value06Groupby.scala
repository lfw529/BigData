package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value06Groupby {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Groupby").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3 具体业务逻辑
    //创建一个 RDD
    var rdd: RDD[Int] = sc.makeRDD(1 to 4, 2)
    //将每个分区的数据放到一个数组并收集到 Driver 端 打印
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(int => {
      if (int % 2 == 0) 0
      else 1
    }) //非省略版，int % 2 本身就有 0，1 两个值
    groupRDD.collect().foreach(println)
    println("----------------------------")
    //TODO 1 按奇偶分组 打印 [省略写法]
    rdd.groupBy(_ % 2).collect().foreach(println)
    println("============================")
    //TODO 2 按大于2的分组，根据结果分区 [false,true]
    rdd.groupBy(_ > 2).collect().foreach(println)
    println("============================")
    //TODO 3 按奇偶分组，并带上分区编号
    groupRDD.mapPartitionsWithIndex((index, iters) => iters.map((index, _))).collect().foreach(println)
    println("============================")
    //创建另一个RDD
    val rdd1: RDD[String] = sc.makeRDD(List("hello", "hive", "hadoop", "spark", "scala"))
    //按照首字母第一个单词相同分组
    rdd1.groupBy(str => str.substring(0, 1)).collect().foreach(println)

    Thread.sleep(Long.MaxValue)
    //5.关闭
    sc.stop()
  }
}
