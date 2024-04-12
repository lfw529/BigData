package com.lfw.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 将两个DataFrame进行join操作
 * leftOuterJoin
 */
object DSLDemo6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SQLContinuedLogin")
      .master("local[*]")
      .getOrCreate()


    //DSL风格的API
    val orderDF: DataFrame = spark.read.json("spark-sql/data/order.json")
    val categoryDF: DataFrame = spark.read.json("spark-sql/data/category.json")

    import spark.implicits._

    //在生成执行计划时，按照代价进行了优化，使用了broadcast join
    val res = orderDF.join(categoryDF, $"cid" === $"id", "left_outer")

    res.show(30)

    //Thread.sleep(100000000)
  }
}
