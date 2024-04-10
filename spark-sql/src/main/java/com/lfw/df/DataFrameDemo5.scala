package com.lfw.df

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataFrameDemo5 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrameDemo5")
      .master("local[*]")
      .getOrCreate() // 创建一个 SparkSession 或只用已有的 SparkSession

    // 创建 RDD
    val lines: RDD[String] = spark.sparkContext.textFile("spark-sql/data/boy.txt")
    // 对 RDD 进行整理
    // RDD
    val tpRDD: RDD[(Int, String, Int, Double)] = lines.map(e => {
      val fields = e.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      (id, name, age, fv) // 将数据封装到元组
    })

    import spark.implicits._
    //元组本身就是case class
    //有schame信息(字段名称、字段类型、能不能为空)
    val df = tpRDD.toDF()

    /**
     * root
     * |-- _1: integer (nullable = false)
     * |-- _2: string (nullable = true)
     * |-- _3: integer (nullable = false)
     * |-- _4: double (nullable = false)
     */
    df.printSchema()

    val df2 = tpRDD.toDF("id", "name", "age", "fv")
    df2.printSchema()

    //释放资源
    spark.stop()
  }
}
