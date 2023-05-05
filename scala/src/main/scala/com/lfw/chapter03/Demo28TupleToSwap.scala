package com.lfw.chapter03

object Demo28TupleToSwap {
  def main(args: Array[String]): Unit = {
    val t = new Tuple2("www.google.com", "www.lfw.com")

    println("交换后的元组：" + t.swap)
  }
}
