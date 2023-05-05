package com.lfw.chapter03

object Demo59GenericsClass {

  //1.定义一个Pair泛型类，该类包含两个字段，且两个字段的类型不固定
  class Pair[T](var a: T, var b: T)

  def main(args: Array[String]): Unit = {
    //2.创建不同类型的Pair泛型类对象，并打印
    //val p1 = new Pair[Int](10, "abc") //报错

    val p1 = new Pair[Int](10, 20)
    println(p1.a, p1.b) //(10, 20)

    val p2 = new Pair[String]("a", "b")
    println(p2.a, p2.b) //(a, b)
  }
}
