package com.lfw.chapter03

object Demo27TupleToString {
  def main(args: Array[String]): Unit = {
    val t = new Tuple3(1, "hello", Console)

    println("连接后的字符串为：" + t.toString()) //连接后的字符串为：(1,hello,scala.Console$@ea4a92b)
  }
}
