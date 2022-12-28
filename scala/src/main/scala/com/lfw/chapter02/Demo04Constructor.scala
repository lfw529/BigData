package com.lfw.chapter02

object Demo04Constructor {
  def main(args: Array[String]): Unit = {
    val student1 = new Student1
    student1.Student1() //1.主构造方法被调用  4.一般方法被调用
    println("=======================")
    val student2 = new Student1("alice") //1.主构造方法被调用  2.辅助构造方法一致被调用  name: alice age: 0
    println("=======================")
    val student3 = new Student1("bob", 25) //1.主构造方法被调用  2.辅助构造方法一致被调用  name: bob age: 0  3.辅助构造方法二被调用  name: bob age: 25
  }
}

class Student1() {
  //定义属性
  var name: String = _
  var age: Int = _

  println("1.主构造方法被调用")

  //声明辅助构造方法
  def this(name: String) {
    this() //直接调用主构造器
    println("2.辅助构造方法一致被调用")
    this.name = name
    println(s"name: $name age: $age")
  }

  def this(name: String, age: Int) {
    this(name)
    println("3.辅助构造方法二被调用")
    this.age = age
    println(s"name: $name age: $age")
  }

  def Student1(): Unit = {
    println("4.一般方法被调用")
  }
}
