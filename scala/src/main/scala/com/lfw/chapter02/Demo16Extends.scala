package com.lfw.chapter02

object Demo16Extends {
  def main(args: Array[String]): Unit = {
    //1.类型的检测和转换
    val student: StudentZ = new StudentZ("alice", 18)
    student.study()  //student study
    student.sayHi()  //hi from student alice
    val person: PersonZ = new PersonZ("bob", 20)
    person.sayHi()  //hi from person bob

    //类型判断
    println("student is StudentZ: " + student.isInstanceOf[StudentZ]) //student is StudentZ: true
    println("student is PersonZ: " + student.isInstanceOf[PersonZ])   //student is PersonZ: true
    println("person is PersonZ: " + person.isInstanceOf[PersonZ])     //person is PersonZ: true
    println("person is StudentZ: " + person.isInstanceOf[StudentZ])   //person is StudentZ: false

    val person2: PersonZ = new PersonZ("cary", 35)
    println("person2 is StudentZ: " + person2.isInstanceOf[StudentZ])  //person2 is StudentZ: false

    //类型转换
    if(person.isInstanceOf[StudentZ]) {
      val newStudent = person.asInstanceOf[StudentZ]
      newStudent.study()
    }
    println(classOf[StudentZ])  //class com.lfw.chapter02.StudentZ

    //2.测试枚举类
    println(WorkDay.MONDAY) //Monday
  }
}

class PersonZ(val name: String, val age: Int) {
  def sayHi(): Unit = {
    println("hi from person " + name)
  }
}

class StudentZ(name: String, age: Int) extends PersonZ(name, age) {
  override def sayHi(): Unit = {
    println("hi from student " + name)
  }

  def study(): Unit = {
    println("student study")
  }
}

//定义枚举类对象
object WorkDay extends Enumeration {
  val MONDAY = Value(1, "Monday")
  val TUESDAY = Value(2, "Tuesday")
}

//定义应用类对象
object TestApp extends App { //相当于java @Test
  println("app start")
  type MyString = String //定义新类型，字符串类型
  val a: MyString = "abc"
  println(a) //abc
}