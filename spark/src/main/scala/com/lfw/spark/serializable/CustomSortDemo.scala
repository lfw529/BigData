package com.lfw.spark.serializable

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSortDemo {
  def main(args: Array[String]): Unit = {

    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sc对象
    val sc = new SparkContext(conf)
    //使用并行化的方式创建RDD
    val lines = sc.parallelize(
      List(
        "laoduan,38,99.99",
        "nianhang,33,99.99",
        "laozhao,18,9999.99"
      )
    )
    val tfBoy: RDD[Boy] = lines.map(line => {
      val fields = line.split(",")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toDouble
      new Boy(name, age, fv) //将数据封装到一个普通的class中
    })

    implicit val ord = new Ordering[Boy] {
      override def compare(x: Boy, y: Boy): Int = {
        if (x.fv == y.fv) {
          x.age - y.age
        } else {
          java.lang.Double.compare(y.fv, x.fv)
        }
      }
    }
    //sortBy会产生shuffle，如果Boy没有实现序列化接口，Shuffle时会报错
    val sorted: RDD[Boy] = tfBoy.sortBy(bean => bean)

    val res = sorted.collect()

    println(res.toBuffer)
  }
}

//如果以后定义bean，建议使用case class
class Boy(val name: String, var age: Int, var fv: Double) //extends Serializable
{
  override def toString = s"Boy($name, $age, $fv)"
}