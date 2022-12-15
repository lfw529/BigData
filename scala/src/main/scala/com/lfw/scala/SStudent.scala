package com.lfw.scala

class SStudent(name: String, age: Int) {
  def printInfo(): Unit = {
    println(name + " " + age + " " + SStudent.school)
  }
}

//引入伴生对象，scala没有static关键字
object SStudent {
  val school: String = "University"

  def main(args: Array[String]): Unit = {
    val alice = new SStudent("alice", 20)
    val bob = new SStudent("bob", 23)

    alice.printInfo()
    bob.printInfo()
  }
}