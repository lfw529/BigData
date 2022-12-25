package com.lfw.chapter01

import scala.collection.immutable
import scala.language.postfixOps

object Demo09ForLoop {
  def main(args: Array[String]): Unit = {
    // java for语法： for(int i = 0; i < 10; i++){ System.out.println(i + ". hello world") }

    //1.范围遍历;  i 表示循环的变量，<- 规定 to
    for (i <- 1 to 10) {
      println(i + ". hello world")
    }
    for (i: Int <- 1.to(10)) {
      println(i + ". hello world")
    }

    println("==========================")
    //Range 与 until 等价，不包括末尾边界
    for (i <- Range(1, 10)) {
      println(i + ". hello world")
    }
    println("**************************")
    for (i <- 1 until 10) {
      println(i + ". hello world")
    }
    println("==========================")
    // 2. 集合遍历：<- 等价与 Java 中 foreach
    for (i <- Array(12, 34, 53)) {
      println(i)
    }
    for (i <- List(12, 34, 53)) {
      println(i)
    }
    for (i <- Set(12, 34, 53)) {
      println(i)
    }
    println("==========================")
    // 3. 循环守卫
    for (i <- 1 to 10) {
      if (i != 5) {
        println(i)
      }
    }
    for (i <- 1 to 10 if i != 5) {
      println(i)
    }
    println("==========================")
    // 4. 循环步长
    for (i <- 1 to 10 by 2) {
      println(i) //1 3 5 7 9
    }
    println("1-------------------------")
    for (i <- 13 to 30 by 3) {
      println(i) //13 16 19 ....
    }

    println("2-------------------------")
    for (i <- 30 to 13 by -2) {
      println(i) //30 28 26 ...
    }
    for (i <- 10 to 1 by -1) {
      println(i)
    }
    println("3-------------------------")
    for (i <- 1 to 10 reverse) {
      println(i) //10 9 8 7 ...
    }
    println("4-------------------------")
    //    for (i <- 30 to 13 by 0){
    //      println(i)
    //    }    // error，step不能为0

    for (data <- 1.0 to 10.0 by 0.3) { //如果要设置步长为小数，则用 1.0 到 10.0
      println(data)
    }
    println("==========================")
    // 5. 循环嵌套
    for (i <- 1 to 3) {
      for (j <- 1 to 3) {
        println("i = " + i + ", j = " + j)
      }
    }
    println("-------------------------")
    for (i <- 1 to 4; j <- 1 to 5) {
      println("i = " + i + ", j = " + j)
    }
    println("==========================")
    // 6. 循环引入变量
    for (i <- 1 to 10) {
      val j: Int = 10 - i
      println("i = " + i + ", j = " + j)
    }

    for (i <- 1 to 10; j = 10 - i) {
      println("i = " + i + ", j = " + j)
    }

    for {
      i <- 1 to 10
      j = 10 - i
    } {
      println("i = " + i + ", j = " + j)
    }
    println("==========================")
    // 7. 循环返回值
    val a: Unit = for (i <- 1 to 10) {
      i
    }
    println("a = " + a) //a = () ,默认情况下没有返回值
    //需通过 yield 关键字将处理结果返回到一个新的 vector
    val b: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield i * i
    println("b = " + b)
  }
}
