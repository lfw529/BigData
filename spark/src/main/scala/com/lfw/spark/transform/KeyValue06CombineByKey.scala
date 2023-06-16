package com.lfw.spark.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object KeyValue06CombineByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("CombineByKey").setMaster("local[*]")
    //创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val lst = List(
      ("spark", 1), ("hadoop", 1), ("hive", 1), ("spark", 1),
      ("spark", 1), ("flink", 1), ("hbase", 1), ("spark", 1),
      ("kafka", 1), ("kafka", 1), ("kafka", 1), ("kafka", 1),
      ("hadoop", 1), ("flink", 1), ("hive", 1), ("flink", 1)
    )

    //通过并行化的方式创建RDD，分区数量为4
    val wordAndOne: RDD[(String, Int)] = sc.parallelize(lst, 4)
    //调用combineByKey传入三个函数
    //val reduced = wordAndOne.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
    val f1 = (x: Int) => {
      val stage = TaskContext.get().stageId()
      val partition = TaskContext.getPartitionId()
      println(s"f1 function invoked in state: $stage, partition: $partition")
      x
    }
    //在每个分区内，将key相同的value进行局部聚合操作
    val f2 = (a: Int, b: Int) => {
      val stage = TaskContext.get().stageId()
      val partition = TaskContext.getPartitionId()
      println(s"f2 function invoked in state: $stage, partition: $partition")
      a + b
    }
    //第三个函数是在下游完成的
    val f3 = (m: Int, n: Int) => {
      val stage = TaskContext.get().stageId()
      val partition = TaskContext.getPartitionId()
      println(s"f3 function invoked in state: $stage, partition: $partition")
      m + n
    }
    val reduced = wordAndOne.combineByKey(f1, f2, f3)
    reduced.collect().foreach(println)

    // 关闭资源
    sc.stop()
  }
}
