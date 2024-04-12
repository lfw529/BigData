package com.lfw.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DSL风格的语法，就是直接调用DataFrame或Dataset上的方法（传入的不是函数，而是一种特殊的表达式），而不是写SQL
 *
 * select filter 属于逐行运算
 */
object DSLDemo1 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("DSLDemo1")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.parquet("spark-sql/data/data.snappy.parquet")

    //df.printSchema()

    //df.show()
    //直接调用DataFrame的方法
    val df2: DataFrame = df.select("uid", "flow")

    import spark.implicits._

    val df3: DataFrame = df.select($"uid", $"flow")

    //一个单引号
    val df4: DataFrame = df.select('uid, 'flow).where($"flow" >= 20)

    val df5: DataFrame = df.select($"uid", 'flow)

    df4.show()
  }
}