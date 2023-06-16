package com.lfw.spark.transform

import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

object KeyValue15ShuffleRDD {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("ShuffleRDD").setMaster("local[*]")
    //利用SparkConf创建sc对象
    val sc = new SparkContext(conf)

    val lst: Seq[(String, Int)] = List(
      ("spark", 1), ("hadoop", 1), ("hive", 1), ("spark", 1),
      ("spark", 1), ("flink", 1), ("hbase", 1), ("spark", 1),
      ("kafka", 1), ("kafka", 1), ("kafka", 1), ("kafka", 1),
      ("hadoop", 1), ("flink", 1), ("hive", 1), ("flink", 1)
    )

    //通过并行化的方式创建RDD，分区数量为4
    val wordAndOne: RDD[(String, Int)] = sc.parallelize(lst, 4)

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
    //指定分区器为HashPartitioner
    val partitioner = new HashPartitioner(wordAndOne.partitions.length)
    val shuffledRDD = new ShuffledRDD[String, Int, Int](wordAndOne, partitioner)
    //设置聚合亲器并关联三个函数
    val aggregator = new Aggregator[String, Int, Int](f1, f2, f3)
    shuffledRDD.setAggregator(aggregator) //设置聚合器
    shuffledRDD.setMapSideCombine(true) //设置map端聚合

    shuffledRDD.collect().foreach(println)
    sc.stop()
  }
}
