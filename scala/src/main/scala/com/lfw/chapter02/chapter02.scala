package com.lfw

package object chapter02 {
  // 定义当前包共享的属性和方法
  val commonValue = "lfw"

  def commonMethod(): Unit = {
    println(s"我是${commonValue}小朋友")
  }
}

