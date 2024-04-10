package com.lfw.df

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.beans.BeanProperty

/**
 * 使用 sparksql 提供的 row 封装数据并关联 Schema
 */
object DataFrameDemo4 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrameDemo4")
      .master("local[*]")
      .getOrCreate() //创建一个SparkSession或使用已有的SparkSession

    //创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("spark-sql/data/boy.txt")
    //对RDD进行整理

    //RDD
    val rowRDD: RDD[Row] = lines.map(e => {
      val fields = e.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv) //使用row封装数据，没有schema
    })

    //定义 Schema, StructType 定了字段名称和类型、能不能为空
    val schema: StructType = StructType.apply(
      List(
        StructField("id", DataTypes.IntegerType),
        StructField("name", DataTypes.StringType, nullable = false),
        StructField("age", DataTypes.IntegerType),
        StructField("fv", DataTypes.DoubleType)
      )
    )

    //将RDD和Schema关联到一起了
    //DataFrame = RDD + Schema + 执行计划 + Encoder
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)

    df.printSchema()

    //执行计划（执行计划的优化规则：基于规则、基于代价）
    val plan = df.queryExecution.executedPlan
    println(plan)

    df.show()

    //释放资源
    spark.stop()
  }
}
