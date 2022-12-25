package com.lfw.chapter01

import scala.util.control.Breaks
import scala.util.control.Breaks.{break, breakable}

object Demo13Breakable {
  def main(args: Array[String]): Unit = {
    // 1. 采用抛出异常的方式，退出循环
    try {
      for (i <- 0 until 5) {
        if (i == 3)
          throw new RuntimeException
        println(i)   // 0 1 2
      }
    } catch {
      case e: Exception => // 什么都不做，只是退出循环
    }
    println("----------------------------")
    // 2. 使用Scala中的Breaks类的break方法，实现异常的抛出和捕捉
    Breaks.breakable(
      for (i <- 0 until 5) {
        if (i == 3)
          Breaks.break()
        println(i)  // 0 1 2
      }
    )
    println("这是循环外的代码")
    println("----------------------------")
    //简化
    /** breakable() 源码解析 */
    //    def breakable(op: => Unit) {
    //      try {
    //        op
    //      } catch {
    //        case ex: BreakControl =>
    //          if (ex ne breakException) throw ex
    //      }
    //    }
    //(1)op:=> Unit 表示接受的参数是一个没有输入，也没有返回值的函数
    //(2)即可以简单理解可以接收一段代码
    //(3)breakable 对 break() 抛出的异常做了处理，代码就继续执行
    //(4)当我们传入的是代码块，scala 程序员会将() 改成{}
    breakable(
      for (i <- 0 until 5) {
        if (i == 3)
          break() // def break(): Nothing = { throw breakException }  抛出异常
        println(i)  //0 1 2
      }
    )
    println("这是循环外的代码")
  }
}
