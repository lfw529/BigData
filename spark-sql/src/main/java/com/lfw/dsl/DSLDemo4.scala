package com.lfw.dsl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.DataTypes

/**
 * DSL风格的语法，就是直接调用DataFrame或Dataset上的方法（传入的不是函数，而是一种特殊的表达式），而不是写SQL
 *
 * 划分窗口运算
 */
object DSLDemo4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DSLDemo4")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read.csv("spark-sql/data/stu.txt").toDF("name", "age", "gender")

    df.createTempView("v_user")
    //    spark.sql(
    //      """
    //        |select
    //        |  name,
    //        |  age,
    //        |  gender,
    //        |  rank() over(partition by gender order by age) rk
    //        |from
    //        |  v_user
    //        |""".stripMargin).show()

    //spark SQL支持的全部函数
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //分完组后，只能对age求平均数据，count是整个DataFrame的条数
    val res = df.select( //子查询先做类型转换
      $"name",
      $"age".cast(DataTypes.IntegerType),
      $"gender"
    ).select(
      $"name",
      $"age",
      $"gender",
      rank().over(Window.partitionBy("gender").orderBy($"age".asc)).as("rk")
    )

    res.show()
  }
}
