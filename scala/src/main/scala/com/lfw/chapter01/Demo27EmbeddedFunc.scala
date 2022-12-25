package com.lfw.chapter01

import scala.annotation.tailrec

object Demo27EmbeddedFunc {
  def main(args: Array[String]): Unit = {
    println(factorial(0))  //1
    println(factorial(1))  //1
    println(factorial(2))  //2
    println(factorial(3))  //6
  }

  def factorial(i: Int): Int = {
    @tailrec
    def fact(i: Int, accumulator: Int): Int = {
      if (i <= 1)
        accumulator
      else
        fact(i - 1, i * accumulator)
    }

    fact(i, 1)
  }
}
