package com.lfw.dsl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object DSLContinuedLogin {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DSLContinuedLogin")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val df: DataFrame = spark.read.csv("spark-sql/data/user-login.txt").toDF("uid", "dt")
    //去重
    val res: DataFrame = df
      .distinct()
      .select(
        $"uid",
        'dt,
        row_number().over(Window.partitionBy("uid").orderBy($"dt".asc)) as "rn"
      ).select(
        $"uid",
        $"dt",
        date_sub($"dt", $"rn") as "date_dif"
      ).groupBy(
        "uid",
        "date_dif"
      ).agg(
        count("*") as "counts",
        min("dt") as "start_dt",
        max("dt") as "end_dt"
      ).drop(
        "date_dif"
      ).where(
        $"counts" >= 3
      )

    res.show()
  }
}