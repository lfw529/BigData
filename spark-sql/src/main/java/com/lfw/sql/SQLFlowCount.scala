package com.lfw.sql

import org.apache.spark.sql.SparkSession

/**
 * lag(column, n, default) over()   向下压几行
 * lead over                        向上抬几行
 */
object SQLFlowCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("SQLFlowCount")
      .master("local[*]")
      .getOrCreate() // 创建一个 SparkSession 或使用已有的 SparkSession

    val df = spark.read
      .csv("spark-sql/data/data.csv")
      .toDF("uid", "start_time", "end_time", "flow")

    df.createTempView("v_user_data")

    val res = spark.sql(
      s"""
         |select
         |  uid,
         |  min(start_time) start_time,
         |  max(end_time) end_time,
         |  sum(flow) flow
         |from
         |(
         |  select
         |    uid,
         |    start_time,
         |    end_time,
         |    flow,
         |    sum(flag) over(partition by uid order by start_time rows between unbounded preceding and current row) sum_flag
         |  from
         |  (
         |    select
         |      uid,
         |      start_time,
         |      end_time,
         |      flow,
         |      if(to_unix_timestamp(start_time, 'yyyy-MM-dd HH:mm:ss') - to_unix_timestamp(lag_time, 'yyyy-MM-dd HH:mm:ss') > 600, 1, 0) flag
         |    from
         |    (
         |      select
         |        uid,
         |        start_time,
         |        end_time,
         |        flow,
         |        lag(end_time, 1, start_time) over(partition by uid order by start_time) lag_time
         |      from
         |        v_user_data
         |    )
         |  )
         |)
         |group by uid, sum_flag
         |""".stripMargin)

    res.show()
  }
}
