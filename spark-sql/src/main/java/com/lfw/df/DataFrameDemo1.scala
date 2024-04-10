package com.lfw.df

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameDemo1 {
  def main(args: Array[String]): Unit = {

    //SparkCore编写程序的过程
    //1.创建 SparkConf，然后创建 SparkContext
    //2.创建 RDD（分布式抽象数据集）
    //3.调用 Transformation(s)
    //4.调用 Action
    //5.释放资源（sc.stop）

    //--------------------------------------

    //SparkSQL 的重要步骤
    //1.创建 SparkSession（对SparkContext包装，增强）
    //2.创建 DataFrame 或 DataSet（增强的分布式数据集，对RDD的进一步包装和增强，里面关联schema）
    //3.写 SQL 或调用 DSL 风格的 API（类似Transformation）
    //4.调用 Action
    //5.释放资源 (sparkSession.stop)
    val spark: SparkSession = SparkSession.builder()
      .appName("DataFrameDemo1")
      .master("local[*]")
      .getOrCreate() //创建一个SparkSession或使用已有的SparkSession

    //创建RDD
    val lines: RDD[String] = spark.sparkContext.textFile("spark-sql/data/boy.txt")
    //对RDD进行整理
    val boyRDD: RDD[Boy] = lines.map(e => {
      val fields = e.split(",")
      val id = fields(0).toInt
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Boy(id, name, age, fv) //关联 case class，就是关联了 schema
    })

    //val df = spark.createDataFrame(boyRDD)

    //导入隐式转换(导入sparkSession对象下的隐式转换)
    import spark.implicits._

    //将RDD转成DataFrame
    //DataFrame/Dataset 是RDD的封装和增强
    //DataFrame/Dataset = RDD + Schema + 执行计划
    val df: DataFrame = boyRDD.toDF()

    //注册视图
    df.createTempView("v_boy")

    //打印schema
    df.printSchema()

    //调用SQL（Transformation）
    //val df2: DataFrame = spark.sql("select * from v_boy order by fv desc, age asc")

    val res = spark.sql("select year(current_date) y, month(current_date) m, day(current_date) d")

    res.show()
    //触发Action
    //df2.show()

    //释放资源
    spark.stop()
  }
}

case class Boy(id: Int, name: String, age: Int, fv: Double)
