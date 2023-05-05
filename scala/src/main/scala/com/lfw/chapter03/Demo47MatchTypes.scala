package com.lfw.chapter03

object Demo47MatchTypes {
  def main(args: Array[String]): Unit = {
    //1.匹配常量
    def describeConst(x: Any): String = x match {
      case 1 => "Int one"
      case "hello" => "String hello"
      case true => "Boolean true"
      case '+' => "Char +"
      case _ => ""
    }

    println(describeConst("hello")) //String hello
    println(describeConst('+')) //Char +
    println(describeConst(0.3)) //空串

    println("1=================================")

    //2.匹配类型
    def describeType(x: Any): String = x match {
      case i: Int => "Int " + i
      case s: String => "String " + s
      case list: List[String] => "List " + list
      case array: Array[Int] => "Array[Int] " + array.mkString(",")
      case a => "Something else: " + a
    }

    println(describeType(35)) //Int 35
    println(describeType("hello")) //String hello
    println(describeType(List("hi", "hello"))) //List List(hi, hello)
    println(describeType(List(2, 23))) //List List(2, 23)
    println(describeType(Array("hi", "hello"))) //Something else: [Ljava.lang.String;@75bd9247
    println(describeType(Array(2, 23))) //Array[Int] 2,23

    println("2=================================")

    //3.匹配数组
    for (arr <- List(
      Array(0),
      Array(1, 0),
      Array(0, 1, 0),
      Array(1, 1, 0),
      Array(2, 3, 7, 15),
      Array("hello", 1, 30),
    )) {
      val result: String = arr match {
        case Array(0) => "0"
        case Array(1, 0) => "Array(1, 0)"
        case Array(x, y) => "Array: " + x + ", " + y //匹配两元素数组
        case Array(0, _*) => "以0开头的数组"
        case Array(x, 1, z) => "中间为1的三元素元组"
        case _ => "something else"
      }
      println(result)
      /*
        0
        Array(1, 0)
        以0开头的数组
        中间为1的三元素数组
        something else
        中间为1的三元素数组
      */
    }
    println("3=================================")

    //4.匹配列表
    //方式1
    for (list <- List(
      List(0),
      List(1, 0),
      List(0, 0, 0),
      List(1, 1, 0),
      List(88),
      List("hello")
    )) {
      val result: String = list match {
        case List(0) => "0"
        case List(x, y) => "List(x, y): " + x + ", " + y //两个元素的list匹配
        case List(0, _*) => "List(0,...)" //以0开头
        case List(a) => "List(a): " + a
        case _ => "something else"
      }
      println(result)
      /*
       0
       List(x, y): 1, 0
       List(0, ...)
       something else
       List(a): 88
       List(a): hello
     */
    }
    println("4=================================")

    //方式2
    val list1 = List(1, 2, 5, 7, 24) //first: 1, second: 2, rest: List(5, 7, 24)

    list1 match {
      case first :: second :: rest => println(s"first: $first, second: $second, rest: $rest")
      case _ => println("something else")
    }
    println("5=================================")

    //5.匹配元组
    for (tuple <- List(
      (0, 1), //case (a, b) => "" + a + ", " + b
      (0, 0), //case (a, b) => "" + a + ", " + b
      (0, 1, 0), // case (a, 1, _) => "(a, 1, _) " + a
      (0, 1, 1), // case (a, 1, _) => "(a, 1, _) " + a
      (1, 23, 56), //case (x, y, z) => "(x, y, z) " + x + " " + y + " " + z
      ("hello", true, 0.5) //case (x, y, z) => "(x, y, z) " + x + " " + y + " " + z
    )) {
      val result: String = tuple match { //注意：如果已经成功匹配到一个，后面的匹配结果就不会再匹配了
        case (a, b) => "" + a + ", " + b
        case (0, _) => "(0, _)"
        case (a, 1, _) => "(a, 1, _) " + a
        case (x, y, z) => "(x, y, z) " + x + " " + y + " " + z
        case _ => "something else"
      }
      println(result)
      /*
        0, 1
        0, 0
        (a, 1, _) 0
        (a, 1, _) 0
        (x, y, z) 1 23 56
        (x, y, z) hello true 0.5
       */
    }
  }
}
