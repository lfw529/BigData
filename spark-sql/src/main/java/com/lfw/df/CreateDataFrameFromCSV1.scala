package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDataFrameFromCSV1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CreateDataFrameFromCSV")
      .master("local[*]")
      .getOrCreate()


    //有Schema信息
    //csv方法也触发Action，默认只读取一行，按照指定的分隔符","进行切分
    val df: DataFrame = spark.read.csv("spark-sql/data/data.csv")

    df.printSchema()
    df.show()

    Thread.sleep(100000000)
  }
}
