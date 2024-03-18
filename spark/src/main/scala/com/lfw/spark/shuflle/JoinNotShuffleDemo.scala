package com.lfw.spark.shuflle

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinNotShuffleDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReduceByKeyNotShuffleDemo").setMaster("local[*]") // 本地模式，开启多线程
    //创建 SparkContext
    val sc = new SparkContext(conf)

    //通过并行化的方式创建一个RDD
    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)), 2)
    //通过并行化的方式再创建一个RDD
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2), ("jerry", 4)), 2)
    //该join一定有shuffle，并且是3个Stage
    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    val rdd11 = rdd1.groupByKey()
    val rdd22 = rdd2.groupByKey()
    //下面的join，没有shuffle
    val rdd33 = rdd11.join(rdd22)
    rdd33.foreach(t => println(t))
    sc.stop();
  }
}
