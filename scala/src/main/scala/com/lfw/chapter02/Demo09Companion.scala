package com.lfw.chapter02

object Demo09Companion {
  def main(args: Array[String]): Unit = {
    //    val student = new StudentC("alice", 18)
    //    student.printInfo()
    val student1 = StudentC.newStudent("alice", 18)
    student1.printInfo() //student: name = alice, age = 18
    //不用再new对象了，直接调用
    val student2 = StudentC.apply("bob", 19)
    student2.printInfo() //student: name = bob, age = 19,
    val student3 = StudentC("bob", 19)
    student3.printInfo() //student: name = bob, age = 19
  }
}

//定义类
class StudentC private(val name: String, val age: Int) {
  def printInfo(): Unit = {
    println(s"student: name = ${name}, age = $age")
  }
}

//伴生对象
object StudentC {
  val school: String = "lfw"

  //定义一个类的对象实例的创建方法 [自己定义]
  def newStudent(name: String, age: Int): StudentC = new StudentC(name, age)

  //等价于上面的scala提供的构造方法
  def apply(name: String, age: Int): StudentC = new StudentC(name, age)
}