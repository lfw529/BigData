package com.lfw.chapter03

object Demo30GetOrElse {
  def main(args: Array[String]): Unit = {
    val a: Option[Int] = Some(5)
    val b: Option[Int] = None

    println("a.getOrElse(0): " + a.getOrElse(0))   //a.getOrElse(0): 5
    println("b.getOrElse(10):" + b.getOrElse(10))  //b.getOrElse(10):10
  }
}
