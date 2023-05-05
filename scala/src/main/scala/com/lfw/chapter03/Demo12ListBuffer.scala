package com.lfw.chapter03

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Demo12ListBuffer {
  def main(args: Array[String]): Unit = {
    //1.创建可变列表 ListBuffer
    val list1: ListBuffer[Int] = new ListBuffer[Int]()
    val list2: ListBuffer[Int] = ListBuffer(12, 53, 75)

    println(list1) //ListBuffer()
    println(list2) //ListBuffer(12, 53, 75)
    println("==============================")

    //2.添加元素
    //尾部添加元素
    list1.append(15, 62)
    println(list1) //ListBuffer(15, 62)
    println("==============================")

    //头部添加元素
    list2.prepend(20)
    println(list2) //ListBuffer(20, 12, 53, 75)
    println("==============================")

    //在索引下标为1的位置加入 19, 22 两个元素
    list1.insert(1, 19, 22)
    println(list1) //ListBuffer(15, 19, 22, 62)
    println("==============================")

    //符号操作：+=: 底层是 prepend 头插入; +=: 底层是 append 尾插入
    31 +=: 96 +=: list1 += 25 += 11
    println(list1)
    println("==============================")

    //3.合并 list
    //++: 查看底层得知，++会 clone() 一个缓存在其中追加，将缓存的新地址赋值给变量 list3
    val list3: mutable.Seq[Int] = list1 ++ list2
    println(list1) //ListBuffer(31, 96, 15, 19, 22, 62, 25, 11)
    println(list2) //ListBuffer(20, 12, 53, 75)
    println(list3) //ListBuffer(31, 96, 15, 19, 22, 62, 25, 11, 20, 12, 53, 75)
    println("==============================")

    //符号：++=: 会直接将 list2(后面的列表) 的内容更改为新的合并后的列表
    list1 ++=: list2
    println(list1) //ListBuffer(31, 96, 15, 19, 22, 62, 25, 11)
    println(list2) //ListBuffer(31, 96, 15, 19, 22, 62, 25, 11, 20, 12, 53, 75)
    println("==============================")

    //4.修改元素
    //直接将索引下标为3的元素值改成30
    list2(3) = 30
    //update 方法修改：将索引下标为0的元素值改成89
    list2.update(0, 89)
    println(list2) //ListBuffer(89, 96, 15, 30, 22, 62, 25, 11, 20, 12, 53, 75)
    println("==============================")

    //5.删除元素
    //remove 方法删除：删除索引下标为2的元素
    list2.remove(2)
    list2.append(25)
    //符号删除：删除列表中值等于25的第一个元素
    list2 -= 25
    println(list2) //ListBuffer(89, 96, 30, 22, 62, 11, 20, 12, 53, 75, 25)
  }
}
