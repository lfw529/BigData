package com.lfw.spark.shuflle

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object ReduceByKeyNotShuffleDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReduceByKeyNotShuffleDemo").setMaster("local[*]") // 本地模式，开启多线程
    //创建 SparkContext
    val sc = new SparkContext(conf)

    //创建 RDD，并没有立即读取数据，而是触发 Action 才会读取数据
    val lines = sc.textFile("hdfs://hadoop102:8020/spark/input/wordcount.txt")

    val wordAndOne = lines.flatMap(_.split(" ")).map((_, 1))

    // 先使用 HashPartitioner 进行 partitionBy
    val partitioner = new HashPartitioner(wordAndOne.partitions.length)
    val partitioned = wordAndOne.partitionBy(partitioner)

    // 然后再调用reduceByKey
    val reduced: RDD[(String, Int)] = partitioned.reduceByKey(_ + _)

    reduced.foreach(t => println(t))
    sc.stop()
  }
}
