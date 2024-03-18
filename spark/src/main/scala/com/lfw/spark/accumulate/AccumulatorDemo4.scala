package com.lfw.spark.accumulate

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用累加器，只触发一次 Action，将想要的结果算出来的同时，再将偶数的数量计算出来
 *
 * 面试题：多次触发 count Action 问题
 */
object AccumulatorDemo4 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AccumulatorDemo3").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    //在Driver定义一个特殊的变量，即累加器
    //Accumulator可以将每个分区的计数结果，通过网络传输到Driver，然后进行全局求和
    val accumulator: LongAccumulator = sc.longAccumulator("even-acc")
    val rdd2 = rdd1.map(e => {
      if (e % 2 == 0) {
        accumulator.add(1) //闭包，在Executor中累计的
      }
      e * 10
    })

    // 如何避免多次触发 Action 计数不断累加？
    //方式1：cache
//    rdd2.cache()

    // 触发一次 Action
    rdd2.saveAsTextFile("spark/out/AccumulatorDemo4")
    println(accumulator.count)   //输出4

    //方式2：reset()
    accumulator.reset()   // 将 Driver 的值进行重置

    // 触发两次 Action
    rdd2.saveAsTextFile("spark/out/AccumulatorDemo5")
    println(accumulator.count)   //输出8
  }
}
