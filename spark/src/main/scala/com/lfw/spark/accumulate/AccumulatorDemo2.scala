package com.lfw.spark.accumulate

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用累加器，只触发一次 Action，将想要的结果算出来的同时，再将偶数的数量计算出来
 * 定义一个普通的变量，是无法将每个 Task 的计数器返回到 Driver
 */
object AccumulatorDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo1").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    //在 Driver 端定义的
    //定义一个普通的变量，是无法将每个 Task 的计数器返回到 Driver
    var acc = 0

    val rdd2 = rdd1.map(e => {
      if (e % 2 == 0) {
        acc += 1 // 闭包
      }
      e * 10
    })

    // 就触发一次 Action
    rdd2.saveAsTextFile("spark/out/AccumulatorDemo2")
    // 每个 Task 中累加的数据会返回到 Driver 端吗？ 不能
    println(acc)
  }
}
