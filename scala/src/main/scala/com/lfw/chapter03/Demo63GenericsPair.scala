package com.lfw.chapter03

object Demo63GenericsPair {
  //1.定义一个Super类和一个Sub子类
  class Super

  class Sub extends Super

  //2.使用协变、逆变、非变分别定义三个泛型类
  class Temp1[T]

  class Temp2[+T]

  class Temp3[-T]

  def main(args: Array[String]): Unit = {
    //3.分别创建泛型类对象来演示协变、逆变、非变
    //测试非变
    val t1: Temp1[Sub] = new Temp1[Sub]
    //val t2:Temp1[Super] = t1 //编译报错

    //测试协变
    val t3: Temp2[Sub] = new Temp2[Sub]
    val t4: Temp2[Super] = t3

    //测试逆变
    //val t5:Temp3[Sub] = new Temp3[Sub]
    //val t6:Temp3[Super] = t5 //编译报错
    val t7: Temp3[Super] = new Temp3[Super]
    val t8: Temp3[Sub] = t7
  }
}

