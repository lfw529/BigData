package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Value07GroupbyWordcount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")
    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)

    //3具体业务逻辑
    // 3.1 创建一个RDD
    val strList: List[String] = List("Hello Scala", "Hello Spark", "Hello World")
    val rdd = sc.makeRDD(strList)
    //3.2 将字符串拆分成一个一个的单词
    val wordRdd: RDD[String] = rdd.flatMap(_.split(" "))
    //3.3 将单词结果进行转换：word => (word,1)
    val wordToOneRdd: RDD[(String, Int)] = wordRdd.map((_, 1))
    //3.4 将转换结构后的数据分组
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = wordToOneRdd.groupBy(_._1)
    //3.5 将分组后的数据进行结构的转换
    //方式1：Lambda 表达式
    /*  val wordToSum: RDD[(String, Int)] = groupRdd.map(
        t => (t._1, t._2.toList.size)
      )*/

    //方式2：模式匹配
    /* val wordToSum = groupRdd.map({
       x =>
         x match {
           case (word, list) => {
             (word, list.size)
           }
         }
     })*/

    //方式3：模式匹配简化:偏函数
    val wordToSum = groupRdd.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //打印输出
    wordToSum.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
