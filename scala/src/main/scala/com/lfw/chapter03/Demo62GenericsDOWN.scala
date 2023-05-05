package com.lfw.chapter03

object Demo62GenericsDOWN {

  //1.定义一个Person类
  class Person

  //2.定义一个Policeman类，继承Person类
  class Policeman extends Person

  //3.定义一个Superman类，继承Policeman类
  class Superman extends Policeman

  //4.定义一个泛型方法demo()，该方法接收一个Array参数
  //def demo[T](arr:Array[T]) = println(arr)
  //5.限定demo方法的Array元素类型只能是Person或Policeman
  def demo[T >: Policeman](arr: Array[T]): Unit = println(arr)

  def main(args: Array[String]): Unit = {
    //6.调用demo()方法，传入不同元素类型的Array
    demo(Array(new Person()))
    demo(Array(new Policeman()))
    //demo(Array(new Superman())) //报错
    //demo(Array("a", "b", "c")) //报错
  }
}
