package com.lfw.chapter03

object Demo58GenericsMethod {
  //方式一：不采用泛型的普通方法
  def getMiddleElement1(arr: Array[Int]): Int = arr(arr.length / 2)

  //方式二：采用自定义的泛型
  //T:type单词的缩写，类型（默认）
  def getMiddleElement2[T](arr: Array[T]): T = arr(arr.length / 2)

  def main(args: Array[String]): Unit = {
    //测试方式一
    println(getMiddleElement1(Array(1, 2, 3, 4, 5))) //3
//    println(getMiddleElement1(Array("a", "b", "c"))) //报错！可不可以把Int向上提升为Any呢？可以但消耗资源

    //测试方式二
    println(getMiddleElement2(Array(1, 2, 3, 4, 5))) //3
    println(getMiddleElement2(Array("a", "b", "c"))) //b
  }
}
