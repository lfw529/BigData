package com.lfw.spark.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Cache01 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Cache").setMaster("local[*]")

    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lineRdd: RDD[String] = sc.textFile("spark/input/cache/")

    val wordRdd: RDD[String] = lineRdd.flatMap(line => line.split(" "))

    val wordToOneRdd = wordRdd.map {
      word => { //懒加载
        (word, 1)
      }
    }

    // cache 缓存前打印血缘关系
    println(wordToOneRdd.toDebugString)

    // 数据缓存
    //cache 底层调用的就是 persist 方法，缓存级别默认用的是 MEMORY_ONLY
    wordToOneRdd.cache()

    // persist 方法可以更改存储级别
//    wordToOneRdd.persist(StorageLevel.MEMORY_AND_DISK_2) //开启此行代码需要注释掉 cache()，只能设置一次存储级别

    val start1 = System.currentTimeMillis()
    // 触发执行逻辑
    wordToOneRdd.collect().foreach(println)
    val end1 = System.currentTimeMillis()
    println("第一次执行耗时：" + (end1 - start1))   //1059

    println("------分界线--------")

    //cache 缓存后打印血缘关系
    //cache 操作会增加血缘关系，不改变原有的血缘关系
    println(wordToOneRdd.toDebugString)

    val start2 = System.currentTimeMillis()
    // 再次触发执行逻辑
    wordToOneRdd.collect().foreach(println)
    val end2 = System.currentTimeMillis()

    println("开启缓存后，第二次执行耗时：" + (end2 - start2))  //54

    Thread.sleep(Long.MaxValue)
    //5.关闭
    sc.stop()

  }
}
