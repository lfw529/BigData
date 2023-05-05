package com.lfw.chapter03

object Demo17ImmutableSet {
  def main(args: Array[String]): Unit = {
    //1.创建set
    val set1 = Set(13, 23, 53, 12, 13, 23, 78)
    println(set1) //Set(78, 53, 13, 12, 23)
    println("===============================")

    //2.添加元素，无序随机添加
    //符号操作：+: 添加元素
    val set2: Set[Int] = set1 + 129
    println(set1) //Set(78, 53, 13, 12, 23)
    println(set2) //Set(78, 53, 13, 129, 12, 23)
    println("===============================")

    //3.合并set
    val set3 = Set(19, 13, 23, 53, 67, 99)
    //符号操作：++: 去重合并，无序
    val set4: Set[Int] = set2 ++ set3
    println(set2) //Set(78, 53, 13, 129, 12, 23)
    println(set3) //Set(53, 13, 67, 99, 23, 19)
    println(set4) //Set(78, 53, 13, 129, 12, 67, 99, 23, 19)
    println("===============================")

    //4.删除元素
    // - : 删除值为13的元素
    val set5: Set[Int] = set3 - 13
    println(set3)  //Set(53, 13, 67, 99, 23, 19)
    println(set5)  //Set(53, 67, 99, 23, 19)
  }
}

