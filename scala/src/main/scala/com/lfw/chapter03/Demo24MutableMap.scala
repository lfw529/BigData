package com.lfw.chapter03

import scala.collection.mutable

object Demo24MutableMap {
  def main(args: Array[String]): Unit = {
    //1.创建map
    val map1: mutable.Map[String, Int] = mutable.Map("a" -> 13, "b" -> 25, "hello" -> 3)
    println(map1) //Map(b -> 25, a -> 13, hello -> 3)
    println(map1.getClass) //class scala.collection.mutable.HashMap
    println("=================================")

    //2.添加元素，无序
    map1.put("c", 5)
    map1.put("d", 9)
    println(map1) //Map(b -> 25, d -> 9, a -> 13, c -> 5, hello -> 3)
    println("=================================")

    //符号操作： +=: 等价于 put 方法
    map1 += (("e", 7))
    println(map1) //Map(e -> 7, b -> 25, d -> 9, a -> 13, c -> 5, hello -> 3)
    println("=================================")

    //3.删除元素
    println(map1("c")) //5
    println("+++++++++++++++++++++++++++++++++")
    //删除方法：remove()
    map1.remove("c")
    println(map1.getOrElse("c", 0)) //0     key 不存在则返回0
    //符号操作：-=: 等价于 remove 方法
    map1 -= "d"
    println(map1) //Map(e -> 7, b -> 25, a-> 13, hello -> 3)
    println("=================================")

    //4.修改元素
    map1.update("c", 5) //没有元素则新增
    map1.update("e", 10) //有元素则更改
    println(map1) //Map(aaa -> 11, b -> 29, hello -> 5)
    println("=================================")

    //5.合并两个Map
    val map2: Map[String, Int] = Map("aaa" -> 11, "b" -> 29, "hello" -> 5)
    //符号操作：++: 合并两个map，在map1的基础上，最后map1变为合并后的结果
    //map1 ++= map2
    println(map1) //Map(e -> 10, b -> 25, a -> 13, c -> 5, hello -> 3)
    println(map2) //Map(aaa -> 11, b -> 29, hello -> 5)
    println("=================================")
    //符号操作：++: 在 map3 上打印合并结果，如果 map1 和 map2 主键有相同的，以 ++ 后面的主键值为准 (覆盖掉前面的)
    val map3: Map[String, Int] = map2 ++ map1
    println(map3) //Map(e -> 10, a -> 13, b -> 25, c -> 5, hello -> 3, aaa -> 11)
  }
}
