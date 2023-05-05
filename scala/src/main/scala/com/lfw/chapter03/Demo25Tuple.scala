package com.lfw.chapter03

object Demo25Tuple {
  def main(args: Array[String]): Unit = {
    //1.创建元组  像如下这种格式就是元组的体现，一个四元组
    val tuple: (String, Int, Char, Boolean) = ("hello", 100, 'a', true)
    println(tuple) //(hello, 100, a, true)
    println("----------------------------")
    //2.访问数据
    println(tuple._1) //hello
    println(tuple._2) //100
    println(tuple._3) //a
    println(tuple._4) //true
    println("----------------------------")
    //根据索引返回元素的值，索引为1对应第二个元素，返回值100
    println(tuple.productElement(1)) //100
    println("----------------------------")
    //3.遍历元组数据
    for (elem <- tuple.productIterator)
      println(elem) //hello 100 a true
    println("----------------------------")
    //4.嵌套元组
    val mulTuple: (Int, Double, String, (Int, String), Int) = (12, 0.3, "hello", (23, "scala"), 29)
    println(mulTuple._4._2) //scala
  }
}
