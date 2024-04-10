package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDataFrameFromParquet {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CreateDataFrameFromParquet")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.parquet("spark-sql/out/par/part-00000-8f88c4ea-7f5e-45f8-a074-a7ec47fb7213-c000.snappy.parquet")

    df.printSchema()

    df.show()

    Thread.sleep(100000000)
  }
}
