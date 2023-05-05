package com.lfw.chapter03

object Demo08ListFill {
  def main(args: Array[String]): Unit = {
    val site: List[String] = List.fill(3)("lfw")  //重复 lfw 3次
    println("site : " + site) //site : List(lfw, lfw, lfw)
    println("===========================================")
    val num: List[Int] = List.fill(10)(2)  //重复元素 2, 10 次
    println("num: " + num) //num: List(2, 2, 2, 2, 2, 2, 2, 2, 2, 2)
  }
}
