package com.lfw.spark.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Cache02ReduceByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("autoCache").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineRdd: RDD[String] = sc.textFile("spark/input/cache")

    val wordRdd = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map {
      word => {
        (word, 1)
      }
    }

    //采用 reduceByKey, 自带缓存。word2oneRDD调用了reduceByKey算子, 底层要走shuffle
    //spark怕在shuffle过程发生 OOM 错误,因此会自动对word2oneRDD启用缓存
    val wordByKeyRDD: RDD[(String, Int)] = wordToOneRdd.reduceByKey(_ + _)
    // cache 操作会增加血缘关系，不改变原有的血缘关系
    println(wordByKeyRDD.toDebugString)

    //触发执行逻辑
    wordByKeyRDD.collect().foreach(println)

    println("------分界线--------")

    println(wordByKeyRDD.toDebugString)

    //再次触发执行逻辑
    wordByKeyRDD.collect().foreach(println)

    Thread.sleep(Long.MaxValue)

    //4.关闭连接
    sc.stop()
  }
}
