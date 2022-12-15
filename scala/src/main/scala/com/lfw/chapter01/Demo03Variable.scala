package com.lfw.chapter01

import com.lfw.scala.SStudent

object Demo03Variable {
  def main(args: Array[String]): Unit = {
    //声明一个变量的通用语法
    var a: Int = 10

    //(1)声明变量时，类型可以省略，编译器自动推导，即类型推导
    var a1 = 10
    val b1 = 23

    //(2)类型确定后，就不能修改，说明scala是强类型语言
    var a2 = 15 //a2类型为Int
    //    a2 = "hello"

    //(3)变量声明时，必须有初始值
    //    var a3: Int

    //(4)在声明/定义一个变量时，可以使用var或者val来修饰，var修饰的变量可改变，val修饰的变量不可改变
    a1 = 12
    //    b1 = 25
    //修饰的对象引用可以改变，val 修饰的则不可改变，但对象的状态（值）却是可以改变的。
    var alice = new SStudent("alice", 20)
    alice = new SStudent("Alice", age = 20)
    alice = null
    val bob = new SStudent("bob", 23)
    //bob.age = 24 //在 SStudent 类中更改传参为变量后，可以更改变量中的值；
    //class SStudent(name: String, var age: Int)，SStudent中修改部分，增加了 var 关键字，使 age 成为成员变量可以调用，若用 val ，则不能更改
    bob.printInfo()
  }
}

