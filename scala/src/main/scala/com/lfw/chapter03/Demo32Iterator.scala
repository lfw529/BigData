package com.lfw.chapter03

object Demo32Iterator {
  def main(args: Array[String]): Unit = {
    val it = Iterator("Baidu", "Google", "Taobao")
    while (it.hasNext) {
      println(it.next()) //Baidu  Google  Taobao
    }
  }
}
