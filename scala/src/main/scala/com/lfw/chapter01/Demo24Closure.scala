package com.lfw.chapter01

object Demo24Closure {
  def main(args: Array[String]): Unit = {
    var factor = 3
    val multiplier = (i: Int) => i * factor
    println("multiplier(1) value = " + multiplier(1)) // 1 * 3 = 3
    println("multiplier(2) value = " + multiplier(2)) // 2 * 3 = 6
  }
}