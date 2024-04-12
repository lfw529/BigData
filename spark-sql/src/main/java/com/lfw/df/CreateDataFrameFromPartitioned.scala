package com.lfw.df

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 从分区的目录中读取数据创建DataFrame
 */
object CreateDataFrameFromPartitioned {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("CreateDataFrameFromPartitioned")
      .master("local[*]")
      .getOrCreate()

    //有Schema信息
    val df: DataFrame = spark.read.json("spark-sql/data/json")

    import spark.implicits._
    val res = df.where($"uid" === 1)

    res.show()
  }
}
