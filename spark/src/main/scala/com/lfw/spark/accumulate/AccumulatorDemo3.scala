package com.lfw.spark.accumulate

import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用累加器，只触发一次 Action，将想要的结果算出来的同时，再将偶数的数量计算出来
 */
object AccumulatorDemo3 {
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

    // 就触发一次 Action
    rdd2.saveAsTextFile("spark/out/AccumulatorDemo3")
    // 每个 Task 中累加的数据会返回到 Driver 端吗？ 能
    println(accumulator.count)
  }
}
