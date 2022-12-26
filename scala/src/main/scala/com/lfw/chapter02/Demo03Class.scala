package com.lfw.chapter02

import scala.beans.BeanProperty

object Demo03Class {
  def main(args: Array[String]): Unit = {
    //创建一个对象
    val student = new Student()
    //    student.name   // error, 不能访问private属性
    println(student.age) //0
    println(student.sex) //null
    student.sex = "female"
    println(student.sex) //female
  }
}

//定义一个类
class Student {
  //定义属性
  private var name: String = "alice"

  @BeanProperty
  var age: Int = _ // _ 在这里代表代表默认值为空
  var sex: String = _ //用 var 表示属性可修改
}
