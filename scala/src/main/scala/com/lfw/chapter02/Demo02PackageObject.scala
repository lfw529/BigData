package com.lfw.chapter02

object Demo02PackageObject {
  def main(args: Array[String]): Unit = {
    commonMethod()
    println(commonValue)
  }
}

package ccc {
  package ddd {
    object Demo02PackageObject {
      def main(args: Array[String]): Unit = {
        println(school) //000
        println(number) //111
      }
    }
  }
  package object ddd { //同一层级
    val number: String = "111"
  }
}

// 定义一个包对象
package object ccc { //包名和包对象须在同一层级，才能相互访问
  val school: String = "000"
}







