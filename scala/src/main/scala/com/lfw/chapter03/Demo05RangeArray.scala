package com.lfw.chapter03

import Array._

object Demo05RangeArray {
  def main(args: Array[String]): Unit = {
    var myList1: Array[Int] = range(10, 20, 2)
    var myList2: Array[Int] = range(10, 20)

    //输出所有数组元素
    for (x <- myList1) {
      print(" " + x) // 10 12 14 16 18
    }
    println()
    for (x <- myList2) {
      print(" " + x) // 10 11 12 13 14 15 16 17 18 19
    }
  }
}
