package com.lfw.chapter02

object Demo10AbstractClass {
  def main(args: Array[String]): Unit = {
    val student = new StudentP
    student.eat()  //person eat  // student eat
    println("===========================")
    student.sleep()  //student sleep
  }
}

// 定义抽象类
abstract class PersonP {
  //非抽象属性
  var name: String = "person"
  //抽象属性
  var age: Int

  //非抽象方法
  def eat(): Unit = {
    println("person eat")
  }

  //抽象方法
  def sleep(): Unit
}

// 定义具体的实现子类
class StudentP extends PersonP {
  // 实现抽象属性和方法
  var age: Int = 18

  def sleep(): Unit = {
    println("student sleep")
  }

  // 重写非抽象属性和方法
  //  override val name: String = "student"
  name = "student"

  override def eat(): Unit = {
    super.eat() //调用父类构造器
    println("student eat")
  }
}