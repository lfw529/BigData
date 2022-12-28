package com.lfw.chapter02

object Demo06DynamicBind {
  def main(args: Array[String]): Unit = {
    val student: PersonX = new StudentX
    println(student.name) //student 属性动态绑定
    student.hello() //hello student
  }
}

class PersonX {
  val name: String = "person"

  def hello(): Unit = {
    println("hello person")
  }
}

class StudentX extends PersonX {
  override val name: String = "student"

  override def hello(): Unit = {
    println("hello student")
  }
}
