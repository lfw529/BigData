package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取CSV文件，直接创建DataFrame，然后将DataFrame对应的数据写成Orc格式
 */
object WriteToOrc {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("WriteToOrc")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    //读取data2文件，文件的第一行是表头
    val df: DataFrame = spark
      .read
      .option("header", "true") //读取第一行数据作为表头
      .option("inferSchema", "true") //推断数据类型
      .csv("spark-sql/data/data2.csv")

    df.write.orc("spark-sql/out/orc")

    Thread.sleep(100000000)
  }
}