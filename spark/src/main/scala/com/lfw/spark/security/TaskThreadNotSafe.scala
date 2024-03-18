package com.lfw.spark.security

import com.lfw.spark.utils.DateUtilObj
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示一个Executor中多个Task，访问一个共享对象 (工具类)，同时有读有写操作，会出现线程不安全的问题
 */
object TaskThreadNotSafe {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TaskThreadNotSafe")
      .setMaster("local[*]") //本地模式，开多个线程
    //1.创建SparkContext
    val sc = new SparkContext(conf)

    val lines = sc.textFile("spark/input/date.txt")

    val timeRDD: RDD[Long] = lines.map(e => {
      //将字符串转成 long 类型时间戳
      //使用自定义的 object 工具类
      val time: Long = DateUtilObj.parse(e)
      time
    })

    val res = timeRDD.collect()
    println(res.toBuffer)
  }
}
