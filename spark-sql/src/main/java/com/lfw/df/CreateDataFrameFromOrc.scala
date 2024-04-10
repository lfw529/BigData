package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDataFrameFromOrc {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CreateDataFrameFromOrc")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.orc("spark-sql/out/orc/part-00000-cc478974-c93c-4386-bd99-a6fc0c05fea0-c000.snappy.orc")

    df.printSchema()

    df.show()

    Thread.sleep(100000000)
  }
}
