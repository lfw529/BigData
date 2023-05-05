package com.lfw.chapter03

object Demo03MulArray {
  def main(args: Array[String]): Unit = {
    //1.创建二维数组 2 * 3
    val array: Array[Array[Int]] = Array.ofDim[Int](2, 3)

    //2.访问元素
    array(0)(2) = 19
    array(1)(0) = 25

    println(array.mkString(","))  //[I@7c30a502,[I@43a25848 访问二维数组中的一维数组，打印地址值
    println("--------------------------")
    for (i <- 0 until array.length; j <- array(i).indices) {
      println(array(i)(j)) // 0 0 19 25 0 0
    }
    println("--------------------------")
    for (i <- array.indices; j <- array(i).indices) {
      print(array(i)(j) + "\t")
      if (j == array(i).length - 1) println()
      //     0   0   19
      //     25  0   0
    }
    println("--------------------------")
    array.foreach(line => line.foreach(println))  // 0 0 19 25 0 0
    println("--------------------------")
    //省略版
    array.foreach(_.foreach(println))  // 0 0 19 25 0 0
  }
}
