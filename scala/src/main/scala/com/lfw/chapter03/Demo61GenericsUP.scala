package com.lfw.chapter03

object Demo61GenericsUP {
  //1.定义一个Person类
  class Person

  //2.定义一个Student类，继承Person类
  class Student extends Person

  //3.定义一个泛型方法demo()，该方法接收一个Array参数
  //def demo[T](arr:Array[T]) = println(arr)

  //4.限定demo方法的Array元素类型只能是Person或Person子类
  def demo[T <: Person](arr: Array[T]): Unit = println(arr)

  def main(args: Array[String]): Unit = {
    //5.调用demo()方法，传入不同元素类型的Array
    demo(Array(new Person(), new Person()))
    demo(Array(new Student(), new Student()))
    //demo(Array("a", "b", "c")) //这行会报错
  }
}
