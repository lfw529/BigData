package com.lfw.chapter01

object Demo07Operator {
  def main(args: Array[String]): Unit = {
    // 1. 算术运算符
    val result1: Int = 10 / 3
    println(result1)

    val result2: Double = 10 / 3
    println(result2)

    val result3: Double = 10.0 / 3
    println(result3.formatted("%5.2f"))

    val result4: Int = 10 % 3
    println(result4)

    // 2. 比较运算符
    val s1: String = "hello"
    val s2: String = new String("hello")

    println(s1 == s2)
    println(s1.equals(s2))
    println(s1.eq(s2))
    println("===================")

    // 3. 逻辑运算符
    def m(n: Int): Int = {
      println("m被调用")
      return n
    }

    val n = 1
    println((4 > 5) && m(n) > 0) //后面部分不会判断

    // 判断一个字符串是否不为空
    def isNotEmpty(str: String): Boolean = {
      //如果按位与 & , s 为空，会发生空指针
      return str != null && !("".equals(str.trim))
    }

    println(isNotEmpty(null)) //false

    // 4. 赋值运算符
    //    var b: Byte = 10
    var i: Int = 12
    //    b += 1
    i += 1
    println(i)
    //    i ++   //scala没有 ++ -- 这种表达方式

    // 5. 位运算符
    val a: Byte = 60
    println(a << 3) //原数左移乘以 2^3次方
    println(a >> 2) //原数右移除以 2^2次方

    val b: Short = -13 //负数情况，符号位不会发生变化，除非是无符号位移
    println(b << 2) // -52
    println(b >> 2) // -4
    println(b >>> 2) //很大的正数

    // 6. 运算符的本质
    val n1: Int = 12
    val n2: Int = 37

    println(n1.+(n2))
    // （1）当调用对象的方法时，.可以省略
    println(n1 + n2)

    println(1.34.*(25))
    // （2）如果函数参数只有一个，或者没有参数，()可以省略
    println(1.34 * 25)
  }
}
