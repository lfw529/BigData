package com.lfw.chapter03

object Demo60GenericsTrait {

  //1.定义泛型特质Logger，该类包含一个变量a和show()方法，它们都是用Logger特质的泛型
  trait Logger[T] {
    val a: T

    def show(b: T)
  }

  //2.定义单例对象ConsoleLogger，继承Logger特质
  object ConsoleLogger extends Logger[String] {
    override val a: String = "lfw"

    override def show(b: String): Unit = println(b)
  }

  def main(args: Array[String]): Unit = {
    //3.打印单例对象ConsoleLogger中的成员
    println(ConsoleLogger.a) //lfw
    ConsoleLogger.show("abc") //abc
  }
}
