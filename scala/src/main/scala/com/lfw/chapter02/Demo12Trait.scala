package com.lfw.chapter02

object Demo12Trait {
  def main(args: Array[String]): Unit = {
    val student: StudentU = new StudentU
    student.sayHello() //hello from: uu hello from: student uu
    student.study() //student uu is studying
    student.dating() //student uu is dating
    student.play() //young people uu is playing
  }
}

// 定义一个父类
abstract class PersonU {
  val name: String = "person"
  var age: Int = 18

  def sayHello(): Unit = {
    println("hello from: " + name)
  }

  def increase(): Unit = {
    println("person increase")
  }
}

//定义一个特质
trait Young {
  // 声明抽象和非抽象属性
  var age: Int
  val name: String = "young"

  //声明非抽象方法
  def play(): Unit = {
    println(s"young people $name is playing")
  }

  //声明抽象方法
  def dating(): Unit
}

class StudentU extends PersonU with Young {
  // 重写冲突的属性
  override val name: String = "uu"

  //实现抽象方法 override 可以省略
  def dating(): Unit = println(s"student $name is dating")

  //自己类方法
  def study(): Unit = println(s"student $name is studying")

  //重写父类方法
  override def sayHello(): Unit = {
    super.sayHello()
    println(s"hello from: student $name")
  }
}