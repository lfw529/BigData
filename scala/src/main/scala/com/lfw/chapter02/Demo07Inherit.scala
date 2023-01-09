package com.lfw.chapter02

object Demo07Inherit {
  def main(args: Array[String]): Unit = {
    val student1: StudentT = new StudentT("alice", 18) //1->2->3
    val student2 = new StudentT("bob", 20, "std001") //1->2->3->4

    student1.printInfo() //Student: alice 18 null
    student2.printInfo() //Student: bob 20 std001

    val teacher = new TeacherT
    teacher.printInfo() //1 ->  Teacher

    def personInfo(person: PersonT): Unit = {
      person.printInfo()
    }

    println("=========================")
    //动态绑定
    val person = new PersonT
    personInfo(student1) //1 -> Student: alice 18 null
    personInfo(teacher) //Teacher
    personInfo(person) //Person: null 0
  }
}

class PersonT() {
  var name: String = _
  var age: Int = _

  println("1.父类的主构造器调用")

  def this(name: String, age: Int) {
    this()
    println("2.父类的辅助构造器调用")
    this.name = name
    this.age = age
  }

  def printInfo(): Unit = {
    println(s"Person: $name $age")
  }
}

//定义子类
class StudentT(name: String, age: Int) extends PersonT(name, age) {
  var stdNo: String = _

  println("3.子类的主构造器调用")

  def this(name: String, age: Int, stdNo: String) {
    this(name, age)
    println("4.子类的辅助构造器调用")
    this.stdNo = stdNo
  }

  override def printInfo(): Unit = {
    println(s"Student: $name $age $stdNo")
  }
}

//定义子类
class TeacherT extends PersonT {
  override def printInfo(): Unit = {
    println(s"Teacher")
  }
}