package com.lfw.df

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取JSON文件，直接创建 DataFrame
 */
object CreateDataFrameFromJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CreateDataFrameFromJSON")
      .master("local[*]")
      .getOrCreate()

    //import spark.implicits._

    //RDD + Schema
    //RDD : 从哪里去取数据
    //Schema : 字段的名称和类型
    //json会触发Action，目的是为了获取json文件的schema
    val df: DataFrame = spark.read.json("spark-sql/data/order2.json")

    //    df.printSchema()
    df.createTempView("v_order")

    val res: DataFrame = spark.sql(
      """
        |select oid, money, province, latitude, longitude from v_order where _corrupt_record is null
        |""".stripMargin)

    res.show()

    //不要退出
    Thread.sleep(100000000)
  }
}
