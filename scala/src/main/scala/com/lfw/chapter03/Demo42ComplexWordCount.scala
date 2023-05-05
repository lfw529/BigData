package com.lfw.chapter03

object Demo42ComplexWordCount {
  def main(args: Array[String]): Unit = {
    val tupleList: List[(String, Int)] = List(
      ("hello", 1),
      ("hello world", 2),
      ("hello scala", 3),
      ("hello spark from scala", 1),
      ("hello flink from scala", 2)
    )

    //思路1：直接展开为普通版本
    val newStringList: List[String] = tupleList.map(
      kv => {
        (kv._1.trim + " ") * kv._2
      }
    )
    println(newStringList)

    //接下来操作与普通版本完全一致
    val wordCountList: List[(String, Int)] = newStringList.
      flatMap(_.split(" ")) //空格分词
      .groupBy((word: String) => word) //按照单词分组
      .map(kv => (kv._1, kv._2.size)) //统计出每个单词的个数
      .toList
      .sortBy(_._2)(Ordering[Int].reverse)
      .take(3)

    println(wordCountList)  //List((hello,9), (scala,6), (from,3))
  }
}
