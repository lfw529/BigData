package com.lfw.demo

import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * 使用混搭的方式编写WordCount（而不是使用RDD）
 */
object Demo02_WordCount {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("Demo02_WordCount")
      .master("local[*]")
      .getOrCreate() //创建一个SparkSession或使用已有的SparkSession

    //DataSet是spark的1.6开始推出的新的抽象数据集，在spark2.0 中将DataFrame的API进行了统一，spark3.0进行进一步升级
    //RDD是抽象数据集，RDD经过一系列转换后，触发Action，会构建DAG生成job，也有直接计划（可以在Stage级别进行优化）
    //DataSet/DataFrame有更多的描写信息（schema）并且还有更强大，更聪明执行计划（基于规则的、基于代价的）
    //DataSet/DataFrame有Encoder，支持特殊的特殊的序列化、反序列化方式，效率高、可以直接对系列化类型的数据在内存进行操作
    val ds: Dataset[String] = spark.read.textFile("spark-sql/data/words.txt")

    //DataSet/DataFrame里面有RDD，就是对RDD的包装增强，一定转成RDD，schema就没有了
    //val lines: RDD[String] = ds.rdd

    import spark.implicits._
    val words: Dataset[String] = ds.flatMap(_.split(" "))

    //val wordAndOne = words.map((_, 1))
    //DataSet没有reduceByKey(),这方法比较底层，用起来不方法
    //wordAndOne.reduceByKey(_+_)

    val res = words.groupBy("value").count()
    res.show()
  }
}
