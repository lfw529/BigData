package com.lfw.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DSL风格的语法，就是直接调用DataFrame或Dataset上的方法（传入的不是函数，而是一种特殊的表达式），而不是写SQL
 *
 * 分组聚合运算
 */
object DSLDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DSLDemo2")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.parquet("spark-sql/data/data.snappy.parquet")

    //统计每一个人总的流量
    //列的裁剪
    val res = df.groupBy("uid")
      .sum("flow") // 如果 groupBy 后，只计算一个聚合类的数据，可以直接调用 sum, count, avg, min, max
      .withColumnRenamed("sum(flow)", "sum_flow")

    res.show()
  }
}
