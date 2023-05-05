package com.lfw.chapter03

import Array._

object Demo04ConcatArray {
  def main(args: Array[String]): Unit = {
    var myList1: Array[Double] = Array(1.9, 2.9, 3.4, 3.5)
    var myList2: Array[Double] = Array(8.9, 7.9, 0.4, 1.5)

    var myList3: Array[Double] = concat(myList1, myList2)

    // 输出所有数组元素
    for (x <- myList3) {
      println(x)
    }
  }
}
