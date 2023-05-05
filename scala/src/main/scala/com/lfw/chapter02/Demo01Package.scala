package com.lfw.chapter02

import com.lfw.chapter02.Inner //外部类访问内部类的属性，需要导包

  // 在同一文件中定义不同的包
object Demo01Package {
  def main(args: Array[String]): Unit = {
    import com.lfw.chapter02.Inner //外部环境下需要导包
    println(Inner.in) //in
  }
}

  //在外层包中定义单例对象
  object Outer {
    var out: String = "out"

    def main(args: Array[String]): Unit = {
      println(Inner.in)  //in
    }
  }

//内层包中定义单例对象
      object Inner {
        var in: String = "in"

        def main(args: Array[String]): Unit = {
          //子包中的类可以直接访问父包中的内容，而无需导包
          println(Outer.out)  //out
          Outer.out = "outer"
          println(Outer.out) //outer
        }
      }