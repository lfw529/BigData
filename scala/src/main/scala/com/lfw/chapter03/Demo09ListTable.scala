package com.lfw.chapter03

object Demo09ListTable {
  def main(args: Array[String]): Unit = {
    //通过给定的函数创建5个元素
    val squares: List[Int] = List.tabulate(6)((n: Int) => n * n)
    println("一维: " + squares) //一维: List(0, 1, 4, 9, 16, 25)
    //创建二维列表
    val mul: List[List[Int]] = List.tabulate(4, 5)(_ * _)
    println("多维: " + mul) //多维: List(List(0, 0, 0, 0, 0), List(0, 1, 2, 3, 4), List(0, 2, 4, 6, 8), List(0, 3, 6, 9, 12))
  }
}
