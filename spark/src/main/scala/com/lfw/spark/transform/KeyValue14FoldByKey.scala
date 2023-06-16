package com.lfw.spark.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object KeyValue14FoldByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("FoldByKey").setMaster("local[*]")
    //T利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    val lst: Seq[(String, Int)] = List(
      ("spark", 1), ("hadoop", 1), ("hive", 1), ("spark", 1),
      ("spark", 1), ("flink", 1), ("hbase", 1), ("spark", 1),
      ("kafka", 1), ("kafka", 1), ("kafka", 1), ("kafka", 1),
      ("hadoop", 1), ("flink", 1), ("hive", 1), ("flink", 1)
    )
    //通过并行化的方式创建RDD，分区数量为4
    val wordAndOne: RDD[(String, Int)] = sc.parallelize(lst, 4)

    //与reduceByKey类似，只不过是可以指定初始值，每个分区应用一次初始值
    val reduced: RDD[(String, Int)] = wordAndOne.foldByKey(100)(_ + _)
    reduced.collect().foreach(println)

    sc.stop()
  }
}
