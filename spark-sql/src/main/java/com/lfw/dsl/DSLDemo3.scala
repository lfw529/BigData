package com.lfw.dsl

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * DSL风格的语法，就是直接调用DataFrame或Dataset上的方法（传入的不是函数，而是一种特殊的表达式），而不是写SQL
 *
 * 分组聚合运算， 分完组后，对多个字段进行聚合运算
 */
object DSLDemo3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DSLDemo3")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.csv("spark-sql/data/stu.txt").toDF("name", "age", "gender")

    //    df.createTempView("v_user")
    //
    //    spark.sql(
    //      """
    //        |select
    //        |  gender,
    //        |  count(*) counts,
    //        |  avg(age) avg_age
    //        |from
    //        |  v_user
    //        |group by gender
    //        |""".stripMargin).show()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    //分完组后，只能对age求平均数据，count是整个DataFrame的条数
    //val res = df.select($"gender", $"age".cast("int")).groupBy("gender").avg("age")


    val res: DataFrame = df.select($"gender", $"age".cast("int"))
      .groupBy("gender")
      .agg(
        count("*") as "counts",
        avg("age") as "avg_age"
      )

    res.show()
    //+------+------+------------------+
    //|gender|counts|           avg_age|
    //+------+------+------------------+
    //|female|     4|             40.75|
    //|  male|     3|16.666666666666668|
    //+------+------+------------------+
  }
}
