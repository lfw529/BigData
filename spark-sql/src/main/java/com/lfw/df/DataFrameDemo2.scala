package com.lfw.df

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.beans.BeanProperty

object DataFrameDemo2 {
  def main(args: Array[String]): Unit = {

    //1.创建SparkSession
    val spark = SparkSession.builder()
      .appName("DataFrameDemo2")
      .master("local[*]")
      .getOrCreate()

    //2.创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("spark-sql/data/boy.txt")
    //2将数据封装到普通的class中
    val boyRDD: RDD[SBoy] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      new SBoy(id, name, age, fv) //字段名称，字段的类型
    })
    import spark.implicits._
    //只能RDD中类型为case class才能使用
    //boyRDD.toDF()     //报错

    //将RDD关联schema
    val df: DataFrame = spark.createDataFrame(boyRDD, classOf[SBoy])

    df.printSchema()

    //写DSL风格的语法（编程的语法，不是函数，而是 sparksql 提供的一种表达式）
    val df2: DataFrame = df.where($"age" >= 18).orderBy($"fv".desc, $"age" asc)

    df2.show()

    //释放资源
    spark.stop()
  }
}

/**
 * 参数前面必须有 var 或 val
 * 普通的 scala 的 class，必须添加给字段添加对应的 getter 方法，在 scala 中，可以 @BeanProperty 注解
 */
class SBoy(val id: Int,
           val name: String,
           @BeanProperty
           val age: Int,
           @BeanProperty
           val fv: Double
          ) {
  def getId(): Int = {
    id
  }

  def getName(): String = {
    name
  }
}