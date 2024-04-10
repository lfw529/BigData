package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取CSV文件，直接创建DataFrame
 */
object CreateDataFrameFromCSV3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SQLContinuedLogin")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    //读取data2文件，文件的第一行是表头
    val df: DataFrame = spark
      .read
      .option("header", "true") //读取第一行数据作为表头
      .option("inferSchema", "true") //推断数据类型，不加这行的话，全是 string 类型
      .csv("spark-sql/data/data2.csv")

    //默认的字段类型为string类型
    df.printSchema()
    df.show()

    Thread.sleep(100000000)
  }
}
