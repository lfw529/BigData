package com.lfw.spark.security

import com.lfw.spark.utils.DateUtilClass
import org.apache.spark.{SparkConf, SparkContext}

object TaskThreadSafe {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TaskThreadNotSafe")
      .setMaster("local[*]") //本地模式，开多个线程
    //1.创建SparkContext
    val sc = new SparkContext(conf)

    val lines = sc.textFile("spark/input/date.txt")

    val timeRDD = lines.mapPartitions(it => {
      //一个Task使用自己单独的 DateUtilClass 实例，不会出现线程不安全问题，缺点是浪费一些内存资源
      val dateUtil = new DateUtilClass
      it.map(e => {
        dateUtil.parse(e)
      })
    })

    val res = timeRDD.collect()
    println(res.toBuffer)
  }
}
