package com.lfw.spark.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Checkpoint01 {
  def main(args: Array[String]): Unit = {

    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Checkpoint").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //启动检查点功能前先设置一下检查点的存储目录
    //需要设置路径，否则抛异常：Checkpoint directory has not been set in the SparkContext
    sc.setCheckpointDir("spark/checkpoint")

    val lineRdd: RDD[String] = sc.textFile("spark/input/cache")

    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd: RDD[(String, Long)] = wordRdd.map {
      word => {
        (word, System.currentTimeMillis())
      }
    }

    //3.5 增加缓存，避免再重新跑一个job做checkpoint
            wordToOneRdd.cache() //3次时间一样，不加 缓存会多执行一次

    //3.4 数据检查点：针对wordToOneRdd做检查点计算
    wordToOneRdd.checkpoint()

    //3.2 触发执行逻辑
    wordToOneRdd.collect().foreach(println)
    /*
      (hello,1686462332655)
      (lifuwen,1686462332655)
      (hello,1686462332655)
      (spark,1686462332655)
      (hello,1686462332655)
      (lifuwen,1686462332655)
      (hello,1686462332655)
      (spark,1686462332655)
     */
    // 会立即启动一个新的job来专门的做checkpoint运算

    println("-----------------------------")

    //再次触发执行逻辑 [走的 checkpoint 缓存好的数据]
    wordToOneRdd.collect().foreach(println)
    /*
      (hello,1686462332996)
      (lifuwen,1686462332997)
      (hello,1686462332997)
      (spark,1686462332997)
      (hello,1686462332996)
      (lifuwen,1686462332997)
      (hello,1686462332997)
      (spark,1686462332997)
     */

    println("-----------------------------")
    //再次执行 [走的 checkpoint 缓存好的数据]
    wordToOneRdd.collect().foreach(println)
    /*
      (hello,1686462332996)
      (lifuwen,1686462332997)
      (hello,1686462332997)
      (spark,1686462332997)
      (hello,1686462332996)
      (lifuwen,1686462332997)
      (hello,1686462332997)
      (spark,1686462332997)
    */
    Thread.sleep(Long.MaxValue)

    //4.关闭连接
    sc.stop()
  }
}
