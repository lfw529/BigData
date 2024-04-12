package com.lfw.df

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object CreateDFFromSetSeqMap {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("").master("local[*]").getOrCreate()

    val seq1 = Seq(1, 2, 3, 4)
    val seq2 = Seq(11, 22, 33, 44)

    val rdd: RDD[Seq[Int]] = spark.sparkContext.parallelize(List(seq1, seq2))

    import spark.implicits._

    val df: DataFrame = rdd.toDF()

    df.printSchema()
    df.show()
    //+----------------+
    //|           value|
    //+----------------+
    //|    [1, 2, 3, 4]|
    //|[11, 22, 33, 44]|
    //+----------------+

    df.selectExpr("value[0]", "size(value)").show()
    //+--------+-----------+
    //|value[0]|size(value)|
    //+--------+-----------+
    //|       1|          4|
    //|      11|          4|
    //+--------+-----------+

    /**
     * set类型数据rdd的编解码
     */
    val set1 = Set("a", "b")
    val set2 = Set("c", "d", "e")
    val rdd2: RDD[Set[String]] = spark.sparkContext.parallelize(List(set1, set2))

    val df2: DataFrame = rdd2.toDF("members")
    df2.printSchema()
    df2.show()
    //+---------+
    //|  members|
    //+---------+
    //|   [a, b]|
    //|[c, d, e]|
    //+---------+

    /**
     * map类型数据rdd的编解码
     */
    val map1 = Map("father" -> "mayun", "mother" -> "tangyan")
    val map2 = Map("father" -> "huateng", "mother" -> "yifei", "brother" -> "sicong")
    val rdd3: RDD[Map[String, String]] = spark.sparkContext.parallelize(List(map1, map2))

    val df3 = rdd3.toDF("jiaren")
    df3.printSchema()
    df3.show()
    //+--------------------+
    //|              jiaren|
    //+--------------------+
    //|{father -> mayun,...|
    //|{father -> huaten...|
    //+--------------------+

    df3.selectExpr("jiaren['mother']", "size(jiaren)", "map_keys(jiaren)", "map_values(jiaren)")
      .show(10, truncate = false)
    //+--------------+------------+-------------------------+------------------------+
    //|jiaren[mother]|size(jiaren)|map_keys(jiaren)         |map_values(jiaren)      |
    //+--------------+------------+-------------------------+------------------------+
    //|tangyan       |2           |[father, mother]         |[mayun, tangyan]        |
    //|yifei         |3           |[father, mother, brother]|[huateng, yifei, sicong]|
    //+--------------+------------+-------------------------+------------------------+

    spark.close()
  }
}