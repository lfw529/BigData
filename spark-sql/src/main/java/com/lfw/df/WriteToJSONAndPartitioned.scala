package com.lfw.df

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 将数据写入到指定的目录下，并且按照指定的字段进行分区
 */
object WriteToJSONAndPartitioned {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WriteToJSONAndPartitioned")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.parquet("spark-sql/data/data.snappy.parquet")

    //对数据进行处理
    df.write
      .partitionBy("uid") //按照指定的字段进行分区
      .mode(SaveMode.Overwrite)
      .json("spark-sql/data/json")
  }
}
