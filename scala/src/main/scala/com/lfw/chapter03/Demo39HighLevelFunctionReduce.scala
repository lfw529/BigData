package com.lfw.chapter03

object Demo39HighLevelFunctionReduce {
  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 4)

    // 1. reduce
    println(list.reduce(_ + _)) //10  1+2+3+4
    println(list.reduceLeft(_ + _)) //10  1+2+3+4
    println(list.reduceRight(_ + _)) //10  4+3+2+1
    println("===========================")

    val list2 = List(3, 4, 5, 8, 10)
    println(list2.reduce(_ - _)) // 3-4-5-8-10   -24
    println(list2.reduceLeft(_ - _)) // 3-4-5-8-10    -24
    println(list2.reduceRight(_ - _)) // 3 - (4 - (5 - (8 - 10)))   6
    println("===========================")

    // 2. fold  带初始值的聚合
    println(list.fold(10)(_ + _)) // 10 + 1 + 2 + 3 + 4    20
    println(list.foldLeft(10)(_ - _)) // 10 - 1 - 2 - 3 - 4    0
    println(list2.foldRight(11)(_ - _)) // 3 - (4 - (5 - (8 - (10 - 11)))),  -5
  }
}
