package com.lfw.df

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 通过JDBC的方式，将MySQL中的数据映射成DataFrame，然后使用Spark进行运算
 *
 * 为什么不直接在MySQL中进行数据分析，因为要分析的数据通常比较大，对MySQL的压力太大，效率低，甚至会拖累业务库，导致业务库不能正常工作
 *
 * sparksql将数据同步处理，然后使用spark对数据进行分析
 *
 */
object CreateDataFrameFromMySQL {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("CreateDataFrameFromMySQL")
      .master("local[*]")
      .getOrCreate() //创建一个SparkSession或使用已有的SparkSession

    val props = new Properties()

    //指定连接驱动类型
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    //mysql用户名
    props.setProperty("user", "root")

    //密码
    props.setProperty("password", "1234")

    val url = "jdbc:mysql://hadoop102:3306/test?characterEncoding=UTF-8"

    //jdbc方法不会触发Action，那么如何获取到schema信息呢？
    //调用spark.read.jdbc在Driver端会连接一次MySQL，获取到MySQL的对应表的schema信息
    val df: DataFrame = spark.read.jdbc(url, "student", props)

    df.printSchema()

    df.show()

    Thread.sleep(1000000000)
  }
}
