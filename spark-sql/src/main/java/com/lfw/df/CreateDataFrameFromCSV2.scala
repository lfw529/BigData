package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}

object CreateDataFrameFromCSV2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CreateDataFrameFromCSV")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add("id", DataTypes.IntegerType)
      .add("start_time", DataTypes.StringType)
      .add("end_time", DataTypes.StringType)
      .add("flow", DataTypes.DoubleType)

    val df = spark.read.schema(schema).csv("spark-sql/data/data.csv")
    df.printSchema()
    df.show()

    Thread.sleep(100000000)
  }
}