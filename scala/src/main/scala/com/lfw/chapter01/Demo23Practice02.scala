package com.lfw.chapter01

object Demo23Practice02 {
  //1.完整写法
  def main(args: Array[String]): Unit = {
    def func(i: Int): String => (Char => Boolean) = {
      def f1(s: String): Char => Boolean = {
        def f2(c: Char): Boolean = {
          if (i == 0 && s == "" && c == '0') false
          else true
        }
        f2
      }
      f1
    }

    println(func(0)("")('0')) //false
    println(func(0)("")('1')) //true
    println(func(23)("")('0')) //true
    println(func(0)("hello")('0')) //true

    println("===========================")

    //2.匿名函数简写,去掉名字,再去掉类型
    def func1(i: Int): String => (Char => Boolean) = {
      s => c => if (i == 0 && s == "" && c == '0') false else true
    }

    println(func1(0)("")('0')) //false
    println(func1(0)("")('1')) //true
    println(func1(23)("")('0')) //true
    println(func1(0)("hello")('0')) //true

    println("===========================")

    //3.柯里化
    def func2(i: Int)(s: String)(c: Char): Boolean = {
      if (i == 0 && s == "" && c == '0') false else true
    }

    println(func2(0)("")('0')) //false
    println(func2(0)("")('1')) //true
    println(func2(23)("")('0')) //true
    println(func2(0)("hello")('0')) //true
  }
}
