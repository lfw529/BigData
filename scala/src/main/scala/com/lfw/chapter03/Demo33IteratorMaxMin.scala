package com.lfw.chapter03

object Demo33IteratorMaxMin {
  def main(args: Array[String]): Unit = {
    val ita = Iterator(20, 40, 2, 50, 69, 90)
    val itb = Iterator(20, 40, 2, 50, 69, 90)

    println("最大元素是：" + ita.max) //最大元素是：90
    println("最小元素是：" + itb.min) //最小元素是：2
  }
}
