package com.lfw.chapter01

object Demo22Practice01 {
  def main(args: Array[String]): Unit = {
    val fun = (i: Int, s: String, c: Char) => {
      if (i == 0 && s == "" && c == '0') false
      else true
    }

    println(fun(0, "", '0')) //false
    println(fun(0, "", '1')) //true
    println(fun(23, "", '0')) //true
    println(fun(0, "hello", '0')) //true
  }
}
