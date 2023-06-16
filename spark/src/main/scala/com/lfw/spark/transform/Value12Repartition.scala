package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value12Repartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Repartition").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //3. 创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6), 3)
    // 重新分区
    val repartitionRdd: RDD[Int] = rdd.repartition(2)
    //4 打印查看对应分区数据
    val indexRdd: RDD[(Int, Int)] = repartitionRdd.mapPartitionsWithIndex(
      (index, datas) => {
        datas.map((index, _))
      }
    )
    //5 打印
    indexRdd.collect().foreach(println)
    //6.关闭
    sc.stop()
  }
}
