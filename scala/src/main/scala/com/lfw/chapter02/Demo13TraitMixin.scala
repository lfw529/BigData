package com.lfw.chapter02

object Demo13TraitMixin {
  def main(args: Array[String]): Unit = {
    val student = new StudentR
    student.study() //student lfw is studying
    student.increase() //student lfw knowledge increased: 1

    student.play() //young people lfw is playing
    student.increase() //student lfw knowledge increased: 2

    student.dating() //student lfw is dating
    student.increase() //student lfw knowledge increased: 3

    println("===========================")
    // 动态混入     //类型为：StudentR with Talent 带特质类
    val studentWithTalent: StudentR with Talent = new StudentR with Talent {
      override def singing(): Unit = println("student is good at singing")

      override def dancing(): Unit = println("student is good at dancing")
    }
    studentWithTalent.sayHello() //hello from: lfw  hello from: student lfw
    studentWithTalent.play() //young people lfw is playing
    studentWithTalent.study() //student lfw is studying
    studentWithTalent.dating() //student lfw is dating
    studentWithTalent.dancing() //student is good at dancing
    studentWithTalent.singing() //student is good at singing
  }
}

trait Knowledge {
  var amount: Int = 0

  def increase(): Unit
}

trait Talent {
  def singing(): Unit

  def dancing(): Unit
}

class StudentR extends PersonU with Young with Knowledge {
  //重写冲突的属性
  override val name: String = "lfw"

  //实现抽象方法
  def dating(): Unit = println(s"student $name is dating")

  //实现自己的特有方法
  def study(): Unit = println(s"student $name is studying")

  //重写父类方法
  override def sayHello(): Unit = {
    super.sayHello()
    println(s"hello from: student $name")
  }

  //实现特质中的抽象方法
  override def increase(): Unit = {
    amount += 1
    println(s"student $name knowledge increased: $amount")
  }
}