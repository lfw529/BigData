package com.lfw.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 将两个DataFrame进行join操作
 */
object DSLDemo5 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DSLDemo5")
      .master("local[*]")
      .getOrCreate()


    //DSL风格的API
    val orderDF: DataFrame = spark.read.json("spark-sql/data/order.json")
    val categoryDF: DataFrame = spark.read.json("spark-sql/data/category.json")

    import spark.implicits._

    //在生成执行计划时，按照代价进行了优化，使用了broadcast join
    val res = orderDF.join(categoryDF, $"cid" === $"id")

    res.show()

    //Thread.sleep(100000000)

  }
}
