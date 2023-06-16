package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value09Sample {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Sample").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //3.1 创建一个RDD
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6))

    // 抽取数据不放回（伯努利算法）  不放回
    // 伯努利算法：又叫0、1分布。例如扔硬币，要么正面，要么反面。
    // 具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    // 第一个参数：抽取的数据是否放回，false：不放回
    // 第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    // 第三个参数：随机数种子

    val sampleRDD = dataRDD.sample(false, 0.5) //按照概率来说应该取3个，但是实际情况不一定，只有0和1是绝对的
    sampleRDD.collect().foreach(println)
    println("------分界线-------")
    // 抽取数据放回（泊松算法） 放回
    // 第一个参数：抽取的数据是否放回，true：放回；false：不放回
    // 第二个参数：重复数据的几率，范围大于等于0.表示每一个元素被期望抽取到的次数
    // 第三个参数：随机数种子
    val sampleRDD1: RDD[Int] = dataRDD.sample(true, 2)
    sampleRDD1.collect().foreach(println)

    //关闭
    sc.stop()
  }
}
