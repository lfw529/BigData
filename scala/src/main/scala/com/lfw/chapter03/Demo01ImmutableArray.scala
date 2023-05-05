package com.lfw.chapter03

object Demo01ImmutableArray {
  def main(args: Array[String]): Unit = {
    //1.创建数组
    val arr: Array[Int] = new Array[Int](5)
    //另一种创建方式
    val arr2: Array[Int] = Array(12, 37, 42, 58, 97)
    println(arr) // 地址值

    //2.访问元素，默认初始值为0
    println(arr(0)) //0
    println(arr(1)) //0
    println(arr(4)) //0
    //    println(arr(5))  //java.lang.ArrayIndexOutOfBoundsException
    println("===================")

    //3.数组的遍历
    //1)普通的 for 循环
    for (i <- 0 until arr2.length) {
      println(arr(i))
    }
    println("+++++++++++++++++++")
    //indices 等价于 0 until arr.length
    for (i <- arr.indices) println(arr(i))

    println("-------------------")

    //2)直接遍历所有元素，增强 for 循环
    for (elem <- arr2) println(elem)

    println("+++++++++++++++++++")

    //3)迭代器
    val iter = arr2.iterator

    while (iter.hasNext) {
      println(iter.next())
    }

    println("+++++++++++++++++++")

    //4)调用foreach方法
    arr2.foreach((elem: Int) => println(elem))

    println("-------------------")

    arr.foreach(println) // 0 0 0 0 0

    println("-------------------")
    //mkString 以 '--' 为分隔符
    println(arr2.mkString("--")) //12--37--42--58--97
    println("===================")

    //4.添加元素：前后都加
    println(arr2.mkString("--")) //12--37--42--58--97
    //尾部增加一个元素: 73
    val newArr: Array[Int] = arr2.:+(73)
    println(newArr.mkString("--")) //12--37--42--58--97--73
    //头部增加一个元素: 30
    val newArr2: Array[Int] = newArr.+:(30)
    println(newArr2.mkString("--")) //30--12--37--42--58--97--73
    //头部和尾部都增加元素
    val newArr3: Array[Int] = newArr2 :+ 15
    val newArr4: Array[Int] = 19 +: 29 +: newArr3 :+ 26 :+ 73
    println(newArr4.mkString(", ")) //19, 29, 30, 12, 37, 42, 58, 97, 73, 15, 26, 73
  }
}

//尾部添加元素
//def :+[B >: T: ClassTag](elem: B): Array[B] = {
//  val currentLength = repr.length
//  val result = new Array[B](currentLength + 1)
//  Array.copy(repr, 0, result, 0, currentLength)
//  result(currentLength) = elem
//  result
//}
// 头部添加元素
//  def +:[B >: T: ClassTag](elem: B): Array[B] = {
//  val currentLength = repr.length
//  val result = new Array[B](currentLength + 1)
//  result(0) = elem
//  Array.copy(repr, 0, result, 1, currentLength)
//  result
//}