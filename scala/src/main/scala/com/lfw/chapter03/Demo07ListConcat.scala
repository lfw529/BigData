package com.lfw.chapter03

object Demo07ListConcat {
  def main(args: Array[String]): Unit = {
    val site1: List[String] = "lfw" :: ("Google" :: ("Baidu" :: Nil))
    val site2: List[String] = "Facebook" :: ("Taobao" :: Nil)

    // 使用 ::: 运算符
    var fruit: List[String] = site1 ::: site2
    println("site1 ::: site2 : " + fruit) //site1 ::: site2 : List(lfw, Google, Baidu, Facebook, Taobao)

    // 使用 List.:::() 方法
    fruit = site1.:::(site2)
    println("site1.:::(site2) : " + fruit) //site1.:::(site2) : List(Facebook, Taobao, lfw, Google, Baidu)

    // 使用 concat 方法
    fruit = List.concat(site1, site2)
    println("List.concat(site1, site2) : " + fruit) //List.concat(site1, site2) : List(lfw, Google, Baidu, Facebook, Taobao)
  }
}
