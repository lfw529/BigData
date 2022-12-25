package com.lfw.chapter01

object Demo28ControlAbstraction {
  def main(args: Array[String]): Unit = {
    //1.传值参数
    def f0(a: Int): Unit = {
      println("a: " + a)
      println("a: " + a)
    }

    def f1(): Int = {
      println("f1调用")
      12
    }

    f0(f1()) //f1调用  a: 12  a: 12

    println("======================")

    //2.传名参数，传递的不再是具体的值，而是代码块；Java 没有传名调用
    def f2(a: => Int): Unit = { //声明方式 => 类型
      println("a: " + a)
      println("a: " + a)
    }

    f2(23)
//    a: 23
//    a: 23
    println("======================")
    f2(f1())
//    f1调用
//    a: 12
//    f1调用
//    a: 12
    println("======================")
    f2({
      println("这是一个代码块")
      29
    })
//    这是一个代码块
//    a: 29
//    这是一个代码块
//    a: 29
  }
}
