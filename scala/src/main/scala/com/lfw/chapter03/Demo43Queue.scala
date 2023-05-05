package com.lfw.chapter03

import scala.collection.immutable.Queue
import scala.collection.mutable

object Demo43Queue {
  def main(args: Array[String]): Unit = {
    //1.创建一个可变队列
    val queue: mutable.Queue[String] = new mutable.Queue[String]()
    //入队操作，可变长参数
    queue.enqueue("a", "b", "c")
    println(queue) //Queue(a, b, c)

    //出队操作
    println(queue.dequeue()) //a
    println(queue) //Queue(b, c)
    println(queue.dequeue()) //b
    println(queue) //Queue(c)
    println("-----------------------------")

    //2.不可变队列
    val queue2: Queue[String] = Queue("a", "b", "c")
    //通过新的变量赋值来调用
    val queue3: Queue[String] = queue2.enqueue("d")
    println(queue2) //Queue(a, b, c)
    println(queue3) //Queue(a, b, c, d)
  }
}
