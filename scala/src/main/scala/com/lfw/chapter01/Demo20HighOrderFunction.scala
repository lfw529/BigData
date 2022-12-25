package com.lfw.chapter01

object Demo20HighOrderFunction {
  def main(args: Array[String]): Unit = {
    //有参
    def f(n: Int): Int = {
      println("f调用")
      n + 1
    }

    //无参
    def fun(): Int = {
      println("fun调用")
      1
    }

    val result: Int = f(123)
    println(result) //f调用  124
    println(fun()) //fun调用  1

    println("===============================")

    //1.函数作为值进行传递
    val f1: Int => Int = f
    //简写
    val f2 = f _

    println(f1) //com.lfw.chapter01.Demo20HighOrderFunction$$$Lambda$5/1510067370@19bb089b
    println("----------")
    println(f1(12)) //f调用  13
    println("----------")
    println(f2) //com.lfw.chapter01.Demo20HighOrderFunction$$$Lambda$6/1908923184@4563e9ab
    println("----------")
    println(f2(35)) //f调用  36
    println("===============================")

    // 2. 函数作为参数进行传递
    // 定义二元计算函数
    def dualEval(op: (Int, Int) => Int, a: Int, b: Int): Int = {
      op(a, b)
    }

    def add(a: Int, b: Int): Int = {
      a + b
    }

    println(dualEval(add, 12, 35)) // 47
    println(dualEval((a, b) => a + b, 12, 35)) // 47
    println(dualEval(_ + _, 12, 35)) // 47

    println("===============================")

    // 3. 函数作为函数的返回值返回
    def f5(): Int => Unit = {
      def f6(a: Int): Unit = {
        println("f6调用 " + a)
      }
      //      f6 _
      f6 // 将函数直接返回
    }

    val f6 = f5()
    println("----------")
    println(f6) //com.lfw.chapter01.Demo20HighOrderFunction$$$Lambda$10/209813603@3f0ee7cb
    println("----------")
    println(f6(25)) //f6调用 25  ()
    println("----------")
    //简写  --> 说明 f6 返回空 Unit --> ()
    println(f5()(25)) //f6调用 25  ()
  }
}
