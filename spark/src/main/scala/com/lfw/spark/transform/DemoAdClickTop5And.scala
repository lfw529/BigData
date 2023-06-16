package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object DemoAdClickTop5And {
  def main(args: Array[String]): Unit = {
    //1. 初始化Spark配置信息并建立与Spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkCoreTest")
    val sc = new SparkContext(sparkConf)
    //2. 读取日志文件，获取原始数据
    val dataRDD: RDD[String] = sc.textFile("spark/input/agent.log")

    //3. 将原始数据进行结构转换string =>(prv-adv,1)     (省份-广告，1)
    val prvAndAdvToOneRDD: RDD[(String, Int)] = dataRDD.map {
      line => {
        val datas: Array[String] = line.split(" ")
        val s: (String, Int) = (datas(1) + "-" + datas(4), 1)
        s
      }
    }

    //4. 将转换结构后的数据进行聚合统计 (prv-adv,1)=>(prv-adv,sum)    (省份-广告，n)
    val prvAndAdvToSumRDD: RDD[(String, Int)] = prvAndAdvToOneRDD.reduceByKey(_ + _)

    //5. 将统计的结果进行结构的转换 (prv-adv,sum）=>(prv,(adv,sum))   (省份，(广告，n))
    val prvToAdvAndSumRDD: RDD[(String, (String, Int))] = prvAndAdvToSumRDD.map {
      case (prvAndAdv, sum) => {
        val ks: Array[String] = prvAndAdv.split("-")
        (ks(0), (ks(1), sum))
      }
    }

    //6. 根据省份对数据进行分组：(prv,(adv,sum)) => (prv, Iterator[(adv,sum)])
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvToAdvAndSumRDD.groupByKey()

    //7. 对相同省份中的广告进行排序（降序），取前5名,包括并列
    //sortBy排序 默认升序排序 拍完以后需要翻转
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2).reverse.take(5)
      }
    )

    //sortWith排序 两两排序  传入一个left 和一个 right  大的在前 小的在后  降序排序
    val resultRDD2: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        val orderList: List[(String, Int)] = iter.toList.sortWith(
          (left, right) => left._2 > right._2
        )

        //双变量控制
        var i = 0
        var lastSum = 0
        val result = new ListBuffer[(String, Int)]
        if (orderList.size <= 5) {
          orderList
        } else {
          breakable {
            for (tup <- orderList) {
              if (tup._2 != lastSum) {
                i += 1
                result += tup
                lastSum = tup._2
              } else {
                result += tup
              }
              if (i == 6) {
                result.remove(result.size - 1)
                break()
              }
            }
          }
          result.toList
        }
      }
    )

    //方法2， 交集
    val resultRDD3 = groupRDD.mapValues(
      iter => {
        val orderList: List[(String, Int)] = iter.toList.sortWith(
          (left, right) => left._2 > right._2
        )
        val resList = orderList.map(_._2).distinct.take(5)

        orderList.filter({
          case (str, count) => {
            resList.contains(count)
          }
        })
      }
    )


    //    val mapValuesRDD1: RDD[List[Int]] = mapValuesRDD.map({
    //      list => list._2.map(v => v._2).distinct.sorted.reverse.take(5)
    //    })
    //
    //
    //    val list2 = mapValuesRDD1.collect()(0)
    //    println(list2 + "111")
    //    println("----------------")
    //
    //    val resultRDD: RDD[List[Int]] = mapValuesRDD1.filter(
    //      action => {
    //
    //        val x: Boolean = mapValuesRDD1.collect()contains(mapValuesRDD.mapValues(
    //          datas => datas.map(v => v._2)))
    //
    //
    //      }
    //    )

    //list2.contains(mapValuesRDD.mapValues(v => v.toList.map(x => x._2)))

    //    val mapValuesRDD1: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(_.toList)
    //
    //    val mapValuesRDD2 = mapValuesRDD1.mapValues({
    //      list => list.map(v => v._2).distinct.sorted.reverse.take(5)
    //    })
    //
    //    val mapValuesRDD3: RDD[List[Int]] = mapValuesRDD2.map({
    //      t2 => t2._2
    //    })
    //
    //    val mapValuesRDD4 = mapValuesRDD1.map {
    //      case (prv,list) => {
    //        (prv,(list.map(v => v._1) ,list.map(v => v._2).distinct.sorted.reverse.take(5)(0)))
    //      }
    //    }


    //8. 将结果打印
    resultRDD.collect().foreach(println)
    println("-----------------------------")
    resultRDD2.collect().foreach(println)  //包含并列
    println("-----------------------------")
    resultRDD3.collect().foreach(println)  //包含并列
    //9.关闭与spark的连接
    sc.stop()
  }
}