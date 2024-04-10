package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取Parquet文件中的数据，经过处理，写成JSON格式
 */
object WriteToJSON {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WriteToJSON")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.parquet("spark-sql/out/par/part-00000-8f88c4ea-7f5e-45f8-a074-a7ec47fb7213-c000.snappy.parquet")

    df.printSchema()

    //对数据进行处理
    df.write.json("spark-sql/out/json")
  }
}
