package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value10Distinct {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Distinct").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val distinctRdd: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1), 5)
    // 3.2 打印去重后生成新的 RDD
    //在 distinct() 方法中有两个重载方法，一个带参数，一个不带参数。不带参数的维持以前分区状态
    //带分区的则以新的分区数为标准.  //参数preservesPartitioning表示是否保留父RDD的partitioner分区信息。
    val rdd2 = distinctRdd.distinct()
    rdd2.collect().foreach(println)  //5 1 6 2 9
    println("-------------------------------------------")
    distinctRdd.map((_, null)).reduceByKey((x, _) => x).map(_._1).collect().foreach(println) //distinct 过程分解
    println("-------------------------------------------")
    val indexRdd = rdd2.mapPartitionsWithIndex((index, items) => {
      items.map((index, _))
    })

    indexRdd.collect().foreach(println)
    Thread.sleep(Long.MaxValue)

    //5.关闭
    sc.stop()
  }
}
