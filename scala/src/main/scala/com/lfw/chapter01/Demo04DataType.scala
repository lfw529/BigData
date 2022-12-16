package com.lfw.chapter01

import com.lfw.scala.Student

object Demo04DataType {
  def main(args: Array[String]): Unit = {
    //1.整型类型
    val a1: Byte = 127
    val a2: Byte = -128
    //    val a3: Byte = 128   error
    val a4 = 12 //整数默认类型为Int
    val a5: Long = 1324135436436L //长整型数值定义,须后加‘l’或‘L’

    val b1: Byte = 10
    val b2: Byte = (10 + 20) //idea编译器不够完善,其实可以运行
    println(b2)

    //    val b3: Byte = b1 + 20  //error 因为b1要在运行时才会赋值，编译时无法判断是否超过 byte 范围
    val b3: Byte = (b1 + 20).toByte //b1强制转换后则可以运行
    println(b3)

    //2.浮点类型
    val f1: Float = 1.2345f
    val d1 = 34.2245

    //3.字符类型
    val c1: Char = 'a'
    println(c1)
    val c2: Char = '9'
    println(c2)

    //格式控制字符
    val c3: Char = '\t' //制表符
    val c4: Char = '\n' //换行符
    println("abc" + c3 + "def")
    println("abc" + c4 + "def")

    //转义字符
    val c5: Char = '\\' //表示 \ 自身
    val c6: Char = '\"' //表示 "
    println("abc" + c5 + "def")
    println("abc" + c6 + "def")

    //字符变量底层保存ascii码
    val i1: Int = c1
    println("i1: " + i1)
    val i2: Int = c2
    println("i2: " + i2)

    val c7: Char = (i1 + 1).toChar
    println(c7)
    val c8: Char = (i2 - 1).toChar
    println(c8)

    //4.布尔类型
    val isTrue: Boolean = true
    println(isTrue)

    //5.空类型
    //5.1空值Unit
    def m1(): Unit = {
      println("m1被调用执行")
    }

    val a: Unit = m1()
    println("a: " + a)

    //5.2空引用Null
    //    val n: Int = null //error  基本类型不能使用null
    var student: Student = new Student("alice", 20)
    student = null
    println(student)

    //5.3Nothing
    def m2(n: Int): Int = {
      if (n == 0)
        throw new NullPointerException
      else
        return n
    }

    //没有正常的返回值，用Nothing
    def m3(n: Int): Nothing = {
      throw new NullPointerException
    }

    val b: Int = m2(0)
    println("b: " + b)
  }
}
