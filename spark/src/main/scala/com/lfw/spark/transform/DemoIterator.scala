package com.lfw.spark.transform

import scala.io.Source

object DemoIterator {
  def main(args: Array[String]): Unit = {
    val source = Source.fromFile("spark/input/wordcount.txt")

    //返回的是迭代器，但是该迭代器还没有开始迭代数据
    val lines: Iterator[String] = source.getLines()

    val filtered: Iterator[String] = lines.filter(line => {
      !line.startsWith("error")
    })

    val upper = lines.map(line => {
      println(111)
      line.toUpperCase
    })

    while (upper.hasNext) {
      val e = upper.next()
      println(e)
    }
  }
}
