package com.lfw.chapter02

object Demo08Singleton {
  def main(args: Array[String]): Unit = {
    val student1 = StudentY.getInstance()
    student1.printInfo()  //student: name = alice, age = 18
    val student2 = StudentY.getInstance()
    student2.printInfo()  //student: name = alice, age = 18

    println(student1)  //com.lfw.chapter02.StudentY@16c0663d
    println(student2)  //com.lfw.chapter02.StudentY@16c0663d
  }
}

class StudentY(val name: String, val age: Int) {
  def printInfo(): Unit = {
    println(s"student: name = ${name}, age = $age")
  }
}

//饿汉式
//object StudentY {
//  private val student: StudentY = new StudentY("alice", 18)
//  def getInstance(): StudentY = student
//}

//懒汉式
object StudentY {
  private var student: StudentY = _

  def getInstance(): StudentY = {
    if (student == null) {
      //如果没有对象实例的话，就创建一个
      student = new StudentY("alice", 18)
    }
    student
  }
}
