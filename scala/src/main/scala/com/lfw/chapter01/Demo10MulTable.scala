package com.lfw.chapter01

object Demo10MulTable {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 9) {
      for (j <- 1 to i) {
        print(s"$j * $i = ${j * i} \t")
      }
      println()
    }
    println("------------------------------------")
    //简写
    for (i <- 1 to 9; j <- 1 to i) {
      print(s"$j * $i = ${j * i} \t")
      if (j == i) println()
    }
  }
}
