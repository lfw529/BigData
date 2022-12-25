package com.lfw.chapter01

import scala.annotation.tailrec

object Demo29MyWhile {
  def main(args: Array[String]): Unit = {
    var n = 10
    //1.常规的while循环
    while (n >= 1) {
      println(n)
      n -= 1
    }
    println("==================")

    //2.用闭包实现一个函数，将代码块作为参数传入，递归调用
    def myWhile(condition: => Boolean): (=> Unit) => Unit = {
      //内存函数需要递归调用,参数就是循环体
      def doLoop(op: => Unit): Unit = {
        if (condition) {
          op //10,9,8,7,...,1        ==> 这里的 op 等价于: println(n) ; n -= 1
          myWhile(condition)(op)
        }
      }

      doLoop _ //返回值, _ 代表任意值
    }

    n = 10
    myWhile(n >= 1) {
      println(n)
      n -= 1
    }
    println("==================")

    //3.用匿名函数实现
    def myWhile2(condition: => Boolean): (=> Unit) => Unit = {
      //内层函数需要递归调用,参数就是循环体
      op => { //匿名函数 | 嵌套
        if (condition) { //if (n >= 1)
          op // ==>  这里的 op 等价于: println(n) ; n -= 1
          myWhile2(condition)(op)
        }
      }
    }

    n = 10
    myWhile2(n >= 1) {
      println(n)
      n -= 1
    }
    println("==================")

    // 4. 用柯里化实现,代替嵌套函数
    @tailrec //尾递归检查
    def myWhile3(condition: => Boolean)(op: => Unit): Unit = {
      if (condition) { //if (n >= 1)
        op  // ==>  这里的 op 等价于: println(n) ; n -= 1
        myWhile3(condition)(op)
      }
    }

    n = 10
    myWhile3(n >= 1) {
      println(n)
      n -= 1
    }
  }
}
