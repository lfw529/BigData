package com.lfw.chapter03

object Demo26TupleIteration {
  def main(args: Array[String]): Unit = {
    val t: (Int, Int, Int, Int) = (4, 3, 2, 1)

    t.productIterator.foreach { i => println("Value =" + i) } // 4 3 2 1
  }
}
