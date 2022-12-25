package com.lfw.chapter01

object Demo14TestFunction {

  // 1.方法可以进行重载和重写，程序可以执行
  def main(): Unit = {
  }

  def main(args: Array[String]): Unit = {
    // 1.Scala 语言可以在任何的语法结构中声明任何语法
    import java.util.Date
    new Date()

    // 2.函数没有重载和重写的概念，程序报错
    def test(): Unit = {
      println("无参，无返回值")
    }

    test()

//    def test(name: String): Unit = {
//      println()
//    }

    //3. Scala 中函数可以嵌套定义
    def test2(): Unit = {
      def test3(name: String): Unit = {
        println("函数可以嵌套定义")
      }
    }
  }
}

