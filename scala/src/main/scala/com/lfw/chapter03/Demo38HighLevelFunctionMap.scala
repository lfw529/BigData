package com.lfw.chapter03

import scala.collection.mutable

object Demo38HighLevelFunctionMap {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //1.过滤 filter
    //选取偶数
    val evenList: List[Int] = list.filter((elem: Int) => {
      elem % 2 == 0
    })
    println(evenList) //List(2, 4, 6, 8)
    //选取奇数
    println(list.filter(_ % 2 == 1)) //List(1, 3, 5, 7, 9)
    println("=======================")

    //2.映射map
    //把集合中每个数乘2
    println(list.map(_ * 2)) //List(2, 4, 6, 8, 10, 12, 14, 16, 18)
    println(list.map((x: Int) => x * x)) //List(1, 4, 9, 16, 25, 36, 49, 64, 81)
    println("=======================")

    //3.扁平化
    val nestedList: List[List[Int]] = List(List(1, 2, 3), List(4, 5), List(6, 7, 8, 9))
    //将外层List中的元素压平，通过 ::: 连接在一起
    val flatList: List[Int] = nestedList.head ::: nestedList(1) ::: nestedList(2)
    println(flatList) //List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    //如果元素过多，上面的写法会很冗余，故用下面写法
    val flatList2: List[Int] = nestedList.flatten
    println(flatList2) //List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    println("=======================")

    //4.扁平映射
    //将一组字符串进行分词，并保存成单词的列表
    val strings: List[String] = List("hello world", "hello scala", "hello java", "we study")
    val splitList: List[Array[String]] = strings.map(_.split(" ")) // 分词
    val flattenList: List[String] = splitList.flatten // 打散扁平化

    println(flattenList) //List(hello, world, hello, scala, hello, java, we, study)

    val flatmapList: List[String] = strings.flatMap(_.split(" "))
    println(flatmapList) //List(hello, world, hello, scala, hello, java, we, study)
    println("========================")

    //5.分组groupBy
    //分成奇偶两组
    val groupMap: Map[Int, List[Int]] = list.groupBy(_ % 2)
    //完善
    val groupMap2: Map[String, List[Int]] = list.groupBy((data: Int) => if (data % 2 == 0) "偶数" else "奇数")

    println(groupMap) //Map(1 -> List(1, 3, 5, 7, 9), 0 -> List(2, 4, 6, 8))
    println(groupMap2) //Map(奇数 -> List(1, 3, 5, 7, 9), 偶数 -> List(2, 4, 6, 8))

    // 给定一组词汇，按照单词的首字母进行分组
    val wordList = List("china", "america", "alice", "canada", "cary", "bob", "japan")
    //通过字符数组 charAt(0) 切分
    println(wordList.groupBy(_.charAt(0))) //Map(b -> List(bob), j -> List(japan), a -> List(america, alice), c -> List(china, canada, cary))
  }
}
