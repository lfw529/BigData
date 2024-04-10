package com.lfw.demo

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo01 {
  def main(args: Array[String]): Unit = {
    /**
     * 使用 sparksql 处理结构化数据
     */

    // 1  获取编程环境 SparkSession
    val session: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("demo01")
      .getOrCreate()

    // 2 加载结构化数据   RDD +  数据结构  =  DataFrame
    val frame: DataFrame = session.read.option("header", "true").csv("spark-sql/data/user")

    /**
     * 3 创建视图  使用sql分析数据
     */
    frame.createTempView("tb_user")
    session.sql(
      """
        |select
        |id,
        |name
        |from
        |tb_user
        |
        |""".stripMargin).show()
    /*    frame.printSchema()  //打印结构
        frame.show()   // 打印结构*/

  }
}
