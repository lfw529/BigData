package com.lfw.spark.action

import org.apache.spark.{SparkConf, SparkContext}

import java.sql.DriverManager

object Action12ForeachPartition {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Foreach").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(
      5, 7, 6, 4,
      9, 6, 1, 7,
      8, 2, 8, 5,
      4, 3, 10, 9
    ), 4)

    rdd1.foreachPartition(it => {
      //先创建好一个连接对象
      val connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/spark?characterEncoding=utf-8", "root", "1234")
      val preparedStatement = connection.prepareStatement("Insert into tb_res values (?)")
      //一个分区中的多条数据用一个连接进行处理
      it.foreach(e => {
        preparedStatement.setInt(1, e)
        preparedStatement.executeUpdate()
      })
      //用完后关闭连接
      preparedStatement.close()
      connection.close()
    })
  }
}
