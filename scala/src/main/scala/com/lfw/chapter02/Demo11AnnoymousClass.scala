package com.lfw.chapter02

object Demo11AnonymousClass {
  def main(args: Array[String]): Unit = {
    //匿名子类的实现，重写实现抽象方法
    val person: PersonI = new PersonI {
      override var name: String = "alice"

      override def eat(): Unit = println("person eat")
    }
    println(person.name) //alice
    person.eat() //person eat
  }
}

// 定义抽象类
abstract class PersonI {
  var name: String

  def eat(): Unit
}