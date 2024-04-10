package com.lfw.sql

import org.apache.spark.sql.SparkSession

/**
 * 1.养成一个写sql的良好习惯
 * 2.sql的三种执行模式（逐行运算、分组聚合运算、窗口运算）
 * 3.sparksql 支持的内置函数：https://spark.apache.org/docs/latest/api/sql/index.html
 */
object ContinuedLogin {
  def main(args: Array[String]): Unit = {

    val days = 3

    val spark = SparkSession.builder()
      .appName("ContinuedLogin")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //创建RDD
    val lineRdd = spark.sparkContext.textFile("spark-sql/data/user-login.txt")
    //对RDD进行整理
    val df = lineRdd.map(e => {
      val fields = e.split(",")
      val uid = fields(0)
      val dt = fields(1)
      (uid, dt)
    }).toDF("uid", "dt")

    df.createTempView("v_user_log")

    //spark.sql("SELECT date_sub('2016-07-30', 1)").show()
    //写sql（Transformation）
    val res = spark.sql(
      s"""
         |select
         |  uid,
         |  min(dt) start_dt,
         |  max(dt) end_dt,
         |  count(*) counts
         |from
         |(
         |  select -- 求日期差值
         |    uid,
         |    dt,
         |    date_sub(dt, rn) date_dif
         |  from
         |  (
         |    select -- 打行号
         |      uid,
         |      dt,
         |      row_number() over(partition by uid order by dt asc) rn
         |    from
         |    (
         |      select distinct -- 去重
         |        uid, -- 用户id
         |        dt -- 日期
         |      from
         |        v_user_log
         |    )
         |  )
         |)
         |group by uid, date_dif having counts >= $days
         |""".stripMargin)

    res.show()
  }
}
