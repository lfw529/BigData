package com.lfw.chapter01

object Demo01Identifier {
  def main(args: Array[String]): Unit = {
    //(1)以字母或下划线开头，后接字母、数字、下划线
    val hello: String = ""
    var Hello123 = ""
    val _abc = 123

    //错误案例
    //    val h-b = ""
    //    val 123abc = 234

    //以操作符开头，且只包含操作符（+ - * 、 # ！等）
    val -+/% = "hello"
    println(-+/%)

    //用反引号`....`包括的任意字符串，即使是 Scala 关键字也可以
    val `if` = "if"
    println(`if`)
  }
}
