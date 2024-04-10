package com.lfw.df

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 使用 java 的 class 创建 DataFrame
 */
object DataFrameDemo3 {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrameDemo3")
      .master("local[*]")
      .getOrCreate() //创建一个SparkSession或使用已有的SparkSession

    // 创建 RDD
    val lines: RDD[String] = spark.sparkContext.textFile("spark-sql/data/boy.txt")
    //对 RDD 进行整理
    val boyRDD: RDD[JBoy] = lines.map(e => {
      val fields = e.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      new JBoy(id, name, age, fv) //关联java class，就是关联了schema
    })

    import spark.implicits._
    //只能RDD中类型为case class才能使用
    //boyRDD.toDF()     //报错

    //将RDD关联schema
    val df: DataFrame = spark.createDataFrame(boyRDD, classOf[JBoy])

    df.printSchema()

    //写 DSL 风格的语法（编程的语法，不是函数，而是 sparksql 提供的一种表达式）
    val df2: DataFrame = df.where($"age" >= 18).orderBy($"fv".desc, $"age" asc)

    df2.show()

    //释放资源
    spark.stop()
  }
}
