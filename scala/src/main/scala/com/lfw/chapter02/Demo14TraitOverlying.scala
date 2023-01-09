package com.lfw.chapter02

object Demo14TraitOverlying {
  def main(args: Array[String]): Unit = {
    val student = new StudentN
    student.increase() //person increase

    //钻石问题特征叠加
    val myFootBall = new MyFootBall
    println(myFootBall.describe()) //my ball is a foot-ball
  }
}

// 定义球类特征
trait Ball {
  def describe(): String = "ball"
}

// 定义颜色特征
trait ColorBall extends Ball {
  var color: String = "red"

  override def describe(): String = color + "-" + super.describe()
}

// 定义种类特征
trait CategoryBall extends Ball {
  var category: String = "foot"

  override def describe(): String = category + "-" + super.describe()
}

//定义一个自定义球的类
class MyFootBall extends CategoryBall with ColorBall {
  //明确重写具体哪个父类的方法  //钻石问题
  override def describe(): String = "my ball is a " + super[CategoryBall].describe()
}

trait KnowledgeN {
  var amount: Int = 0

  def increase(): Unit = {
    println("knowledge increased")
  }
}

trait TalentN {
  def singing(): Unit

  def dancing(): Unit

  def increase(): Unit = {
    println("talent increased")
  }
}

class StudentN extends PersonU with TalentN with KnowledgeN {
  override def singing(): Unit = println("singing")

  override def dancing(): Unit = println("dancing")

  override def increase(): Unit = {
    super[PersonU].increase()
  }
}
