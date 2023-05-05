package com.lfw.chapter03

object Demo34IteratorLength {
  def main(args: Array[String]): Unit = {
    val ita = Iterator(20, 40, 2, 50, 69, 90)
    val itb = Iterator(20, 40, 2, 50, 69, 90)

    println("ita.size 的值: " + ita.size) //6
    println("itb.length 的值: " + itb.length) //6
  }
}
