package com.lfw.chapter03

object Demo23ImmutableMap {
  def main(args: Array[String]): Unit = {
    //1.创建map
    val map1: Map[String, Int] = Map("a" -> 13, "b" -> 25, "hello" -> 3)
    println(map1) //Map(a -> 13, b-> 25, hello -
    println("=================================")
    println(map1.getClass) //class scala.collection.immutable.Map$Map3
    println("=================================")

    //2.遍历元素
    map1.foreach(println)  //(a,13) (b,25) (hello,3)
    //完整写法，打印
    println("=================================")
    map1.foreach((kv: (String, Int)) => println(kv))  //(a,13) (b,25) (hello,3)
    println("=================================")

    //3.取map中所有的key或者value，循环遍历
    for (key <- map1.keys) {
      println(s"$key ---> ${map1.get(key)}")
      //a ---> Some(13)
      //b ---> Some(25)
      //hello ---> Some(3)
    }
    println("=================================")
    //4.访问某一个key的value
    println("a: " + map1.get("a").get) //a: 13  最后一个 .get 去掉 Some 包装
    println("c: " + map1.get("c")) //c: None 返回None表示没有该键
    //getOrElse 如果不存在 "c" 则返回 0
    println("c: " + map1.getOrElse("c", 0)) //c: 0
    //直接访问获取值
    println(map1("a")) //13
  }
}
