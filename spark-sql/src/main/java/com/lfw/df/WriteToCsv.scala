package com.lfw.df

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 读取Parquet文件中的数据，经过处理，写成Csv格式
 */
object WriteToCsv {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WriteToCsv")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.parquet("spark-sql/out/par/part-00000-8f88c4ea-7f5e-45f8-a074-a7ec47fb7213-c000.snappy.parquet")

    df.printSchema()

    //对数据进行处理
    df.write
      //.mode(SaveMode.ErrorIfExists) //如果数据存在就报错
      //.mode(SaveMode.Ignore) //没有指定的目录就写，有就啥都不干
      .mode(SaveMode.Append)
      //没有指定的目录就写，有就在原来的目录中追加
      //.mode(SaveMode.Overwrite) //覆盖以前的数据
      .option("header", "true")
      //指定数据表头
      .option("delimiter", "|")
      //字段的分隔符
      .csv("spark-sql/out/csv")
  }
}
