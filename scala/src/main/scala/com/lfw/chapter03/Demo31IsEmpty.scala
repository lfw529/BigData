package com.lfw.chapter03

object Demo31IsEmpty {
  def main(args: Array[String]): Unit = {
    val a: Option[Int] = Some(5)
    val b: Option[Int] = None

    println("a.isEmpty: " + a.isEmpty) //a.isEmpty: false
    println("b.isEmpty: " + b.isEmpty) //b.isEmpty: true
  }
}
