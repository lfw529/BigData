package com.lfw.chapter01

// 打印输出一个九层妖塔
object Demo11Pyramid {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 9) {
      val stars: Int = 2 * i - 1
      val spaces: Int = 9 - i
      println(" " * spaces + "*" * stars)
    }
    println("---------------------------")
    //简化一
    for (i <- 1 to 9; stars = 2 * i - 1; spaces = 9 - i) {
      println(" " * spaces + "*" * stars)
    }
    println("---------------------------")
    //简化二
    for (stars <- 1 to 17 by 2; spaces = (17 - stars) / 2) {
      println(" " * spaces + "*" * stars)
    }
  }
}
