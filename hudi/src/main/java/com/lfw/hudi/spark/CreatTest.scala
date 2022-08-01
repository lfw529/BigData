package com.lfw.hudi.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CreatTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")
    //2.创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //引入 spark.implicits._用于将RDD隐式转换为 DataFrame
    import spark.implicits._
    val createDF = spark.sql(
      """
        |create table hudi_cow_nonpcf_tbl (
        |  uuid int,
        |  name string,
        |  price double
        |) using hudi;
        |""".stripMargin
    )
    spark.sql("show create table hudi_cow_nonpcf_tbl")
  }
}
