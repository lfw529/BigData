package com.lfw.chapter03

object Demo50MatchCaseClass {
  def main(args: Array[String]): Unit = {
    val student: Student1 = Student1("alice", 18)

    //针对对象实例的内容进行匹配
    val result: String = student match {
      case Student1("alice", 18) => "Alice, 18"
      case _ => "else"
    }

    println(result)  //Alice, 18
  }
}

//定义样例类，省略了 apply 和 unapply 方法
case class Student1(str: String, i: Int)
