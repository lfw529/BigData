package com.lfw.spark.task

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// wordcount 任务划分研究
object TaskDivisionTest01 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Checkpoint").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineRdd: RDD[String] = sc.textFile("spark/input/taskdivision/")

    lineRdd.flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _).saveAsTextFile("spark/output/taskdivision")

    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
