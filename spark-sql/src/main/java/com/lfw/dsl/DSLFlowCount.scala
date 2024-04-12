package com.lfw.dsl

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object DSLFlowCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DSLFlowCount")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df: DataFrame = spark.read.parquet("spark-sql/data/data.snappy.parquet")

    val res = df.select(
      $"uid",
      $"start_time",
      $"end_time",
      $"flow",
      expr("lag(end_time, 1, start_time)").over(Window.partitionBy("uid").orderBy("start_time")) as "lag_time"
    ).select(
      $"uid",
      $"start_time",
      $"end_time",
      $"flow",
      expr("if(to_unix_timestamp(start_time, 'yyyy-MM-dd HH:mm:ss') - to_unix_timestamp(lag_time, 'yyyy-MM-dd HH:mm:ss') > 600, 1, 0) flag")
    ).select(
      $"uid",
      $"start_time",
      $"end_time",
      $"flow",
      sum($"flag").over(Window.partitionBy("uid").orderBy("start_time").rowsBetween(Window.unboundedPreceding, Window.currentRow)) as "sum_flag"
    ).groupBy(
      "uid",
      "sum_flag"
    ).agg(
      min("start_time") as "start_time",
      max("end_time") as "end_time",
      sum("flow") as "flow"
    ).drop(
      "sum_flag"
    )

    res.show()
  }
}
