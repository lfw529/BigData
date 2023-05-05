package com.lfw.chapter03

//可变set与不可变set变量名一样，所以一定要注意导包情况

import scala.collection.mutable

object Demo18MutableSet {
  def main(args: Array[String]): Unit = {
    //1.创建set
    val set1: mutable.Set[Int] = mutable.Set(13, 23, 53, 12, 13, 23, 78)
    println(set1) //Set(12, 78, 13, 53, 23)
    println("=====================================")

    //2.添加元素
    //符号操作： +: 向集合添加指定元素, 无序
    val set2: mutable.Set[Int] = set1 + 11 + 999
    println(set2) //Set(12, 999, 78, 13, 53, 11, 23)
    println("=====================================")

    //一般用于直接对本集合进行修改, 添加一个单一元素 11
    set1 += 11
    println(set1) //Set(12, 78, 13, 53, 11, 23)

    //add() 方法添加元素，用于可变集合
    val flag1: Boolean = set1.add(10)
    println(flag1) //返回 true, 代表成功添加
    println(set1) //Set(12, 78, 13, 53, 10, 11, 23)
    val flag2: Boolean = set1.add(10)
    println(flag2) //返回false, 代表该元素已经存在, 无法重复添加
    println(set1) //Set(12, 78, 13, 53, 10, 11, 23)
    println("=====================================")

    //3.删除元素
    //符号操作：-: 向集合删除指定元素
    set1 -= 11
    println(set1) //Set(12, 78, 13, 53, 10, 11, 23)
    //remove() 方法删除元素：删除集合中值为10的元素
    val flag3: Boolean = set1.remove(10)
    println(flag3) //删除成功，返回 true
    println(set1) //Set(12, 78, 13, 53, 23)
    val flag4: Boolean = set1.remove(10)
    println(flag4) //删除失败，返回false，因为集合已经不存在元素了
    println(set1) //Set(12, 78, 13, 53, 23)
    println("=====================================")

    //4.合并两个Set
    val set3: mutable.Set[Int] = mutable.Set(13, 12, 13, 27, 98, 29)
    println(set1)  //Set(12, 78, 13, 53, 23)
    println(set3)  //Set(12, 27, 13, 29, 98)
    println("=====================================")
    //符号操作： ++: 两集合去重合并
    val set4: mutable.Set[Int] = set1 ++ set3
    println(set1)  //Set(12, 78, 13, 53, 23)
    println(set3)  //Set(12, 27, 13, 29, 98)
    println(set4)  //Set(12, 27, 78, 13, 53, 29, 98, 23)
    println("=====================================")
    //符号操作：++= 两集合去重合并, 且直接在 set3(第一个集合中修改)
    set3 ++= set1
    println(set1)  //Set(12, 78, 13, 53, 23)
    println(set3)  //Set(12, 27, 78, 13, 53, 29, 98, 23)
    println("=====================================")
    //5. 打印集合
    set3.foreach(println)
    println(set3.mkString(","))  //12,27,78,13,53,29,98,23
  }
}
