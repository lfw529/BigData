package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KeyValue04GroupByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("GroupByKey").setMaster("local[*]")
    //利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lst = List(
      ("spark", 1), ("hadoop", 1), ("hive", 1), ("spark", 1),
      ("spark", 1), ("flink", 1), ("hbase", 1), ("spark", 1),
      ("kafka", 1), ("kafka", 1), ("kafka", 1), ("kafka", 1),
      ("hadoop", 1), ("flink", 1), ("hive", 1), ("flink", 1)
    )
    //通过并行化的方式创建RDD，分区数量为4
    val wordAndOne: RDD[(String, Int)] = sc.parallelize(lst, 4)
    //按照key进行分组 [并没有聚合]
    val grouped: RDD[(String, Iterable[Int])] = wordAndOne.groupByKey()

    grouped.collect().foreach(println)

    //关闭资源
    sc.stop()
  }
}
