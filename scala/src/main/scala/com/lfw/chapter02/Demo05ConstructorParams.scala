package com.lfw.chapter02

object Demo05ConstructorParams {
  def main(args: Array[String]): Unit = {
    val student2 = new Student2
    student2.name = "alice"
    student2.age = 18
    println(s"student2: name = ${student2.name}, age = ${student2.age}") //student2: name = alice, age = 18

    val student3 = new Student3("bob", 20)  //可以调用属性
    println(s"student3: name = ${student3.name}, age = ${student3.age}") //student3: name = bob, age = 20

    val student4 = new Student4("cary", 25)
    //    println(s"student4: name = ${student4.name}, age = ${student4.age}")  //error,无法调用局部属性
    student4.printInfo() //student4: name = cary, age = 25

    val student5 = new Student5("bob", 20)  //值已确定
    println(s"student5: name = ${student5.name}, age = ${student5.age}") //student5: name = bob, age = 20

    val student6 = new Student6("cary", 25, "lfw")
    println(s"student6: name = ${student6.name}, age = ${student6.age}")  //student6: name = cary, age = 25
    student6.printInfo()  //student6: name = cary, age = 25, school = lfw
  }
}

// 定义类
// 无参构造器
class Student2 {
  // 单独定义属性
  var name: String = _
  var age: Int = _
}

// 上面定义等价于
class Student3(var name: String, var age: Int) //加了 var 就不是局部变量了

// 主构造器参数无修饰 [且满足外部能调用需求写法]
class Student4(name: String, age: Int) {
  def printInfo() {
    println(s"student4: name = ${name}, age = $age")
  }
}

// 这种写法与 Java 无区别
//class Student4(_name: String, _age: Int){
//  var name: String = _name
//  var age: Int = _age
//}

class Student5(val name: String, val age: Int)

class Student6(var name: String, var age: Int) {
  var school: String = _

  def this(name: String, age: Int, school: String) { //辅助构造器
    this(name, age)
    this.school = school
  }

  def printInfo() {
    println(s"student6: name = ${name}, age = $age, school = $school")
  }
}